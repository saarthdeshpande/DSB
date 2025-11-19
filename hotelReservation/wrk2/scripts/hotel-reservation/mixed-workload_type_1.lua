local socket = require("socket")
local json = require("dkjson")
math.randomseed(socket.gettime() * 1000)
math.random(); math.random(); math.random()

-- Change this if you want to hit a different base URL
local url = "http://localhost:5000"

-- Per-thread file path for RPS / latency logs
local rps_file_path = ""

-- Stats state
local request_counts = {}
local latency_buckets = {}                 -- still per-interval (can be left unused)
local cumulative_latency_buckets = {}      -- NEW: running latencies (per key)
local send_interval = 15
local last_send_time = socket.gettime()

-- FIFO queues: one entry per *outstanding* request
local start_times = {}
local keys = {}

-- Unique filename per thread
init = function(args)
  local co = coroutine.running()
  local co_str = tostring(co)
  local thread_id = tonumber(string.match(co_str, "thread: 0x%x+ (%d+)")) or math.random(10000, 99999)
  rps_file_path = string.format("cpu_memory_/rps.txt", thread_id)
end

-- Same behavior as before (id repeated 10x)
local function get_user()
  local id = math.random(0, 500)
  local user_name = "Cornell_" .. tostring(id)
  local pass_word = string.rep(tostring(id), 10)
  return user_name, pass_word
end

local function percentile(sorted, p)
  if #sorted == 0 then return 0 end
  local rank = p * (#sorted - 1) + 1
  local lower = math.floor(rank)
  local upper = math.ceil(rank)
  local weight = rank - lower
  if upper > #sorted then return sorted[#sorted] end
  return sorted[lower] * (1 - weight) + sorted[upper] * weight
end

local function flush_rps_file()
  local now = socket.gettime()
  local parts = {}
  local total_rps = 0

  for key, count in pairs(request_counts) do
    -- CHANGED: use cumulative_latency_buckets instead of per-interval latency_buckets
    local lat_list = cumulative_latency_buckets[key] or {}
    table.sort(lat_list)
    local rps = count / send_interval
    local p95 = percentile(lat_list, 0.95)
    local p99 = percentile(lat_list, 0.99)
    local avg = 0
    for _, v in ipairs(lat_list) do avg = avg + v end
    if #lat_list > 0 then avg = avg / #lat_list end
    total_rps = total_rps + rps
    table.insert(parts, string.format("%s_rps:%.1f;%s_p95:%.1f;%s_p99:%.1f;%s_avg:%.1f",
      key, rps, key, p95, key, p99, key, avg))
  end

  local f = io.open(rps_file_path, "a")
  if f then
    f:write(string.format("total_rps:%.1f; %s\n", total_rps, table.concat(parts, "; ")))
    f:close()
  end

  -- Reset *only* per-interval state
  request_counts = {}
  latency_buckets = {}
end

-- =========================
-- Endpoint generators
-- (unchanged)
-- =========================

-- Wrapper kept for compatibility; calls the new randomized search.
local function hotels()
  return search_hotel()
end

function search_hotel()
  local in_date = math.random(9, 23)
  local out_date = math.random(in_date + 1, 24)

  local in_date_str = (in_date <= 9) and ("2015-04-0" .. in_date) or ("2015-04-" .. in_date)
  local out_date_str = (out_date <= 9) and ("2015-04-0" .. out_date) or ("2015-04-" .. out_date)

  local lat = 38.0235 + (math.random(0, 481) - 240.5) / 1000.0
  local lon = -122.095 + (math.random(0, 325) - 157.0) / 1000.0

  local method = "GET"
  local path_only = "/hotels"
  local full_path = url .. path_only ..
    "?inDate=" .. in_date_str ..
    "&outDate=" .. out_date_str ..
    "&lat=" .. tostring(lat) .. "&lon=" .. tostring(lon)

  local key = string.format("('%s','%s')", path_only, method)

  -- Push into FIFO queues
  table.insert(start_times, socket.gettime())
  table.insert(keys, key)

  return wrk.format(method, full_path, {}, nil)
end

function recommend()
  local coin = math.random()
  local req_param = (coin < 0.33) and "dis" or ((coin < 0.66) and "rate" or "price")

  local lat = 38.0235 + (math.random(0, 481) - 240.5) / 1000.0
  local lon = -122.095 + (math.random(0, 325) - 157.0) / 1000.0

  local method = "GET"
  local path_only = "/recommendations"
  local full_path = url .. path_only ..
    "?require=" .. req_param ..
    "&lat=" .. tostring(lat) .. "&lon=" .. tostring(lon)

  local key = string.format("('%s','%s')", path_only, method)

  table.insert(start_times, socket.gettime())
  table.insert(keys, key)

  return wrk.format(method, full_path, {}, nil)
end

function reserve()
  local in_date = math.random(9, 23)
  local out_date = in_date + math.random(1, 5)

  local in_date_str = (in_date <= 9) and ("2015-04-0" .. in_date) or ("2015-04-" .. in_date)
  local out_date_str = (out_date <= 9) and ("2015-04-0" .. out_date) or ("2015-04-" .. out_date)

  local lat = 38.0235 + (math.random(0, 481) - 240.5) / 1000.0
  local lon = -122.095 + (math.random(0, 325) - 157.0) / 1000.0

  local hotel_id = tostring(math.random(1, 80))
  local user_id, password = get_user()
  local cust_name = user_id
  local num_room = "1"

  local method = "POST"
  local path_only = "/reservation"
  local full_path = url .. path_only ..
    "?inDate=" .. in_date_str ..
    "&outDate=" .. out_date_str ..
    "&lat=" .. tostring(lat) .. "&lon=" .. tostring(lon) ..
    "&hotelId=" .. hotel_id .. "&customerName=" .. cust_name ..
    "&username=" .. user_id .. "&password=" .. password ..
    "&number=" .. num_room

  local key = string.format("('%s','%s')", path_only, method)

  table.insert(start_times, socket.gettime())
  table.insert(keys, key)

  return wrk.format(method, full_path, {}, nil)
end

function user_login()
  local user_name, password = get_user()

  local method = "POST"
  local path_only = "/user"
  local full_path = url .. path_only ..
    "?username=" .. user_name .. "&password=" .. password

  local key = string.format("('%s','%s')", path_only, method)

  table.insert(start_times, socket.gettime())
  table.insert(keys, key)

  return wrk.format(method, full_path, {}, nil)
end

-- =========================
-- REQUEST / RESPONSE / DONE
-- =========================

request = function()
  -- Ratios for each endpoint
  local search_ratio    = 0.60
  local recommend_ratio = 0.39
  local user_ratio      = 0.005
  local reserve_ratio   = 0.005  -- sums to 1.0 with the others

  local coin = math.random()
  if coin < search_ratio then
    return search_hotel()
  elseif coin < search_ratio + recommend_ratio then
    return recommend()
  elseif coin < search_ratio + recommend_ratio + user_ratio then
    return user_login()
  else
    return reserve()
  end
end

response = function(status, headers, body)
  local now = socket.gettime()

  -- Pop oldest outstanding request (FIFO)
  local start = table.remove(start_times, 1)
  local key   = table.remove(keys, 1)

  if not start or not key then
    -- Shouldn't happen, but avoid crashing if it does
    return
  end

  local duration_ms = (now - start) * 1000

  request_counts[key] = (request_counts[key] or 0) + 1

  -- per-interval (kept, though no longer used for percentiles)
  latency_buckets[key] = latency_buckets[key] or {}
  table.insert(latency_buckets[key], duration_ms)

  -- NEW: cumulative per-key latencies (used for percentiles)
  cumulative_latency_buckets[key] = cumulative_latency_buckets[key] or {}
  table.insert(cumulative_latency_buckets[key], duration_ms)

  if now - last_send_time >= send_interval then
    flush_rps_file()
    last_send_time = now
  end
end

done = function(summary, latency, requests)
  flush_rps_file()
end

