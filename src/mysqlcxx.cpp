#include <mysqlcxx.h>

#include <queue>
#include <format>
#include <sstream>
#include <variant>


#ifdef MARIADB_VERSION_ID
#define CONNECT_STRING "SET NAMES utf8mb4, @@SESSION.max_statement_time=3000"
#else
#define CONNECT_STRING "SET NAMES utf8mb4, @@SESSION.max_execution_time=3000"
#endif

namespace mysqlcxx {

  static f64 time_fractional() {
    using namespace std::chrono;
    auto time_point = system_clock::now() + 0ns;
    return static_cast<double>(time_point.time_since_epoch().count()) / 1000000000.0;
  }

  struct BACKGROUND_QUERY {
    std::string  format{};
    QUERY_PARAMS params{};
  };

  constexpr size_t             POOL_SIZE = 16;
  SQLCONN                      connections[POOL_SIZE];

  size_t                       current_index{};

  SQLCONN                      background_connection{};

  u64                          processed{};
  u64                          errored{};

  std::mutex                   b_db_mutex{};

  std::queue<BACKGROUND_QUERY> background_queries{};

  std::thread*                 background_thread = nullptr;

  SQLCONN_LAST_ERROR           last_error{};

  SQL_TABLE                    real_query(SQLCONN& conn, const std::string_view query_format,
                                          const QUERY_PARAMS& query_params);

  SQLCONN_STATS                get_stats() {
    SQLCONN_STATS stats{};

    for (size_t cc = 0; cc < POOL_SIZE; ++cc) {
      SQLCONN&     c = connections[cc];
      SQLCONN_INFO ci{};

      ci.is_ready          = !c.is_busy;
      ci.queries_processed = c.queries_processed;
      ci.queries_errored   = c.queries_errored;
      ci.busy_time         = c.busy_time;
      ci.avg_query_length  = c.avg_query_length;
      ci.is_background     = false;
      stats.connections.push_back(ci);
    }

    stats.queries_processed = processed;
    stats.queries_errored   = errored;

    {
      std::lock_guard<std::mutex> db_lock(b_db_mutex);
      stats.background_queue_length = background_queries.size();
    }

    SQLCONN_INFO ci;

    ci.is_ready          = !background_connection.is_busy;
    ci.queries_errored   = background_connection.queries_errored;
    ci.queries_processed = background_connection.queries_processed;
    ci.busy_time         = background_connection.busy_time;
    ci.avg_query_length  = background_connection.avg_query_length;
    ci.is_background     = true;

    stats.connections.push_back(ci);

    return stats;
  }

  std::optional<SQLCONN_LAST_ERROR> get_last_error() {
    if (last_error.message.empty()) {
      return std::nullopt;
    }
    auto result = last_error;
    last_error.message.clear();
    return result;
  }

  void background_procedure() {
    while (1) {
      std::queue<BACKGROUND_QUERY> bg_copy{};

      {
        std::lock_guard<std::mutex> db_lock(b_db_mutex);

        while (background_queries.size()) {
          bg_copy.emplace(background_queries.front());
          background_queries.pop();
        }
      }

      if (bg_copy.empty()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        continue;
      }

      while (!bg_copy.empty()) {
        BACKGROUND_QUERY q = bg_copy.front();

        real_query(background_connection, q.format, q.params);
        bg_copy.pop();

        processed++;
        background_connection.queries_processed++;
      }
    }
  }

  bool connect(const std::string_view host, const std::string_view user,
               const std::string_view password, const std::string_view db, u32 port) {
    std::lock_guard<std::mutex> db_lock(b_db_mutex);

    bool                        failed = false;

    for (size_t i = 0; i < POOL_SIZE; ++i) {
      if (mysql_init(&connections[i].connection) != nullptr) {
        mysql_options(&connections[i].connection, MYSQL_SET_CHARSET_NAME, "utf8mb4");

        mysql_options(&connections[i].connection, MYSQL_INIT_COMMAND, CONNECT_STRING);

        char reconnect = 1;

        if (mysql_options(&connections[i].connection, MYSQL_OPT_RECONNECT, &reconnect) == 0) {
          if (!mysql_real_connect(&connections[i].connection, host.data(), user.data(),
                                  password.data(), db.data(), port, NULL,
                                  CLIENT_MULTI_RESULTS | CLIENT_MULTI_STATEMENTS)) {
            failed = true;

            last_error = {.thread_id = std::this_thread::get_id(),
                          .message   = std::format("Database connection failed: {}",
                                                   mysql_error(&connections[i].connection))};
            break;
          }
        }
      }
    }

    if (mysql_init(&background_connection.connection) != nullptr) {
      mysql_options(&background_connection.connection, MYSQL_SET_CHARSET_NAME, "utf8mb4");
      mysql_options(&background_connection.connection, MYSQL_INIT_COMMAND, CONNECT_STRING);
      char reconnect = 1;
      if (mysql_options(&background_connection.connection, MYSQL_OPT_RECONNECT, &reconnect) == 0) {
        background_thread = new std::thread(background_procedure);
        return !failed &&
            mysql_real_connect(&background_connection.connection, host.data(), user.data(),
                               password.data(), db.data(), port, NULL,
                               CLIENT_MULTI_RESULTS | CLIENT_MULTI_STATEMENTS);
      }
    }

    return !failed;
  }

  bool close() {
    for (size_t i{}; i < POOL_SIZE; ++i) {
      std::lock_guard<std::mutex> db_lock(connections[i].mutex);
      mysql_close(&connections[i].connection);
    }
    mysql_close(&background_connection.connection);
    return true;
  }

  void query_detach(const std::string_view query_format, const QUERY_PARAMS& query_params) {
    std::lock_guard<std::mutex> db_lock(b_db_mutex);
    background_queries.emplace(BACKGROUND_QUERY{query_format.data(), query_params});
  }

  SQL_TABLE query(const std::string_view query_format, const QUERY_PARAMS& query_params) {
    size_t c{};
    size_t tries{};
    if (current_index >= POOL_SIZE) {
      current_index = c = 0;
    } else {
      c = current_index++;
    }
    while (tries < POOL_SIZE + 1 && connections[c].is_busy) {
      // skipped busy connection!
      c = ++current_index;
      if (current_index >= POOL_SIZE) {
        c = current_index = 0;
      }
      tries++;
    }
    processed++;
    connections[c].queries_processed++;
    return real_query(connections[c], query_format, query_params);
  }

  SQL_TABLE real_query(SQLCONN& conn, const std::string_view query_format,
                       const QUERY_PARAMS& query_params) {
    std::vector<std::string> escaped_parameters;

    SQL_TABLE                rv;

    for (const auto& param : query_params) {
      // every character becomes two, plus NULL terminator
      std::visit(
          [query_params, &escaped_parameters, &conn](const auto& p) {
            std::ostringstream v;
            v << p;
            std::string s_param(v.str());
            char        out[s_param.length() * 2 + 1];
            if (mysql_real_escape_string(&conn.connection, out, s_param.c_str(),
                                         s_param.length()) != (size_t)-1) {
              escaped_parameters.push_back(out);
            }
          },
          param);
    }

    if (query_params.size() != escaped_parameters.size()) {
      last_error = {.thread_id = std::this_thread::get_id(),
                    .message =
                        std::format("Parameter wasn't escaped: {}", mysql_error(&conn.connection))};
      errored++;
      conn.queries_errored++;
      return rv;
    }

    u32         param{};
    std::string query_string{};

    for (auto v = query_format.begin(); v != query_format.end(); ++v) {
      if (*v == '?' && escaped_parameters.size() >= param + 1) {
        query_string.append(escaped_parameters[param]);
        if (param != escaped_parameters.size() - 1) {
          param++;
        }
      } else {
        query_string += *v;
      }
    }

    {
      conn.is_busy                           = true;
      f64                         busy_start = time_fractional();
      std::lock_guard<std::mutex> db_lock(conn.mutex);
      i32 query_result = mysql_query(&conn.connection, query_string.c_str());

      if (query_result == 0) {
        MYSQL_RES* result_set = mysql_use_result(&conn.connection);
        if (result_set) {
          MYSQL_ROW row;
          while ((row = mysql_fetch_row(result_set))) {
            MYSQL_FIELD* fields = mysql_fetch_fields(result_set);
            SQL_ROW      this_row;
            if (mysql_num_fields(result_set) == 0) {
              break;
            }
            if (fields && mysql_num_fields(result_set)) {
              u32 field_count = 0;
              while (field_count < mysql_num_fields(result_set)) {
                std::string field_name = (fields[field_count].name ? fields[field_count].name : "");
                std::string field_value = (row[field_count] ? row[field_count] : "");

                // TODO: handle unsigned flag
                bool is_unsigned = fields[field_count].flags & UNSIGNED_FLAG;
                switch (fields[field_count].type) {
                  case MYSQL_TYPE_TINY:
                  case MYSQL_TYPE_SHORT:
                    this_row[field_name] = static_cast<i16>(std::stoi(field_value));
                    break;
                  case MYSQL_TYPE_INT24:
                    this_row[field_name] = static_cast<i32>(std::stoi(field_value));
                    break;
                  case MYSQL_TYPE_LONG:
                    this_row[field_name] = static_cast<i64>(std::stol(field_value));
                    break;
                  case MYSQL_TYPE_LONGLONG:
                    this_row[field_name] = static_cast<i64>(std::stoll(field_value));
                    break;
                  case MYSQL_TYPE_FLOAT:
                    this_row[field_name] = static_cast<f32>(std::stof(field_value));
                    break;
                  case MYSQL_TYPE_DOUBLE:
                    this_row[field_name] = static_cast<f64>(std::stod(field_value));
                    break;
                  case MYSQL_TYPE_STRING:
                  case MYSQL_TYPE_VAR_STRING:
                  case MYSQL_TYPE_BLOB:
                  default:
                    this_row[field_name] = field_value;
                    break;
                }

                field_count++;
              }
              rv.push_back(this_row);
            }
          }
          mysql_free_result(result_set);
        }
      } else {
        // watch your code.
        last_error = {.thread_id = std::this_thread::get_id(),
                      .message =
                          std::format("SQL Error: {} on query {}",
                                      std::string(mysql_error(&conn.connection)), query_string)};
        errored++;
        conn.queries_errored++;
      }
      conn.busy_time += (time_fractional() - busy_start);
      conn.avg_query_length -= conn.avg_query_length / conn.queries_processed;
      conn.avg_query_length += (time_fractional() - busy_start) / conn.queries_processed;
      conn.is_busy = false;
    }
    return rv;
  }
}
