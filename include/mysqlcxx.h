#pragma once

#include <mysql/mysql.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <map>
#include <mutex>
#include <variant>
#include <thread>
#include <optional>

namespace mysqlcxx {
  // maybe i should hide these...
  using u8  = uint8_t;
  using u16 = uint16_t;
  using u32 = uint32_t;
  using u64 = uint64_t;

  using i8  = int8_t;
  using i16 = int16_t;
  using i32 = int32_t;
  using i64 = int64_t;

  using f32 = float;
  using f64 = double;

  using SQL_PARAM_TYPE = std::variant<f32, f64, u32, u64, i64, i32, i16, bool, std::string>;

  using SQL_ROW      = std::map<std::string, SQL_PARAM_TYPE>;
  using SQL_TABLE    = std::vector<SQL_ROW>;
  using QUERY_PARAMS = std::vector<SQL_PARAM_TYPE>;

  struct SQLCONN {
    MYSQL      connection{};
    std::mutex mutex{};
    u64        queries_processed = 0;
    u64        queries_errored   = 0;

    f64        avg_query_length = 0.0;
    f64        busy_time        = 0.0;

    bool       is_busy = false;
  };

  struct SQLCONN_INFO {
    u64  queries_processed = 0;
    u64  queries_errored   = 0;

    f64  avg_query_length = 0.0;
    f64  busy_time        = 0.0;

    bool is_ready      = true;
    bool is_background = false;
  };

  struct SQLCONN_STATS {
    std::vector<SQLCONN_INFO> connections;

    u64                       queries_processed = 0;
    u64                       queries_errored   = 0;

    u64                       background_queue_length = 0;
  };

  struct SQLCONN_LAST_ERROR {
    std::thread::id thread_id{};
    std::string     message{};
  };

  SQLCONN_STATS                     get_connection_stats();
  std::optional<SQLCONN_LAST_ERROR> get_last_error();

  bool      connect(const std::string_view host, const std::string_view user,
                    const std::string_view password, const std::string_view database, u32 port);

  bool      close();

  SQL_TABLE query(const std::string_view query_format, const QUERY_PARAMS& query_params);

  void      query_detach(const std::string_view query_format, const QUERY_PARAMS& query_params);

} // namespace mysqlcxx
