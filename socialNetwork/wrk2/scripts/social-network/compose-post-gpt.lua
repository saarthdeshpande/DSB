local socket = require("socket")
math.randomseed(socket.gettime()*1000)
math.random(); math.random(); math.random()

-- queue for request starts (time + key)
local starts = {}
local head, tail = 1, 0
local function qpush(v) tail = tail + 1; starts[tail] = v end
local function qpop()
  if head > tail then return nil end
  local v = starts[head]; starts[head] = nil; head = head + 1; return v
end

-- (kept) labels captured in request(), but response uses the popped key
local current_method = ""
local current_path = ""

-- per-interval counters (for RPS)
local request_counts = {}
local latency_buckets = {}

-- cumulative (since-start) stores for running percentiles
local cumulative_latency_buckets = {}          -- per (path,method)
local cumulative_all_latencies = {}            -- across all keys

local send_interval = 15
local last_send_time = socket.gettime()
local rps_file_path = ""

-- Unique filename per thread
init = function(args)
  local co = coroutine.running()
  local co_str = tostring(co)
  local thread_id = tonumber(string.match(co_str, "thread: 0x%x+ (%d+)")) or math.random(10000, 99999)
  rps_file_path = string.format("cpu_memory_/rps.txt", thread_id)
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

local function avg_of(list)
  if #list == 0 then return 0 end
  local s = 0
  for i = 1, #list do s = s + list[i] end
  return s / #list
end

local function flush_rps_file()
  local parts = {}
  local total_rps = 0

  -- Iterate cumulative data; print running p95/p99/avg; RPS is per-interval.
  for key, lat_list in pairs(cumulative_latency_buckets) do
    local rps = (request_counts[key] or 0) / send_interval
    total_rps = total_rps + rps

    if #lat_list > 0 then
      table.sort(lat_list) -- ok to sort in place
      local p95 = percentile(lat_list, 0.95)
      local p99 = percentile(lat_list, 0.99)
      local avg = avg_of(lat_list)
      table.insert(parts, string.format(
        "%s_rps: %.1f; %s_p95: %.1f; %s_p99: %.1f; %s_avg: %.1f",
        key, rps, key, p95, key, p99, key, avg
      ))
    end
  end

  -- (overall running stats are computed but not emitted, kept as-is)

  local f = io.open(rps_file_path, "a")
  if f then
    local line = table.concat(parts, "; ")
    if line ~= "" then
      local f2 = io.open(rps_file_path, "a")
      if f2 then f2:write(line .. "\n"); f2:close() end
    end
    f:close()
  end

  -- Reset per-interval only
  request_counts = {}
  latency_buckets = {}
end

local charset = {'q','w','e','r','t','y','u','i','o','p','a','s',
  'd','f','g','h','j','k','l','z','x','c','v','b','n','m','Q',
  'W','E','R','T','Y','U','I','O','P','A','S','D','F','G','H',
  'J','K','L','Z','X','C','V','B','N','M','1','2','3','4','5',
  '6','7','8','9','0'}

local decset = {'1','2','3','4','5','6','7','8','9','0'}

-- load env vars
local max_user_index = tonumber(os.getenv("max_user_index")) or 962

local function stringRandom(length)
  if length > 0 then
    return stringRandom(length - 1) .. charset[math.random(1, #charset)]
  else
    return ""
  end
end

local function decRandom(length)
  if length > 0 then
    return decRandom(length - 1) .. decset[math.random(1, #decset)]
  else
    return ""
  end
end

request = function()
  current_method = "POST"
  current_path = "/wrk2-api/post/compose"

  local user_index = math.random(0, max_user_index - 1)
  local username = "username_" .. tostring(user_index)
  local user_id = tostring(user_index)
  local text = stringRandom(256)
  local num_user_mentions = math.random(0, 5)
  local num_urls = math.random(0, 5)
  local num_media = math.random(0, 4)
  local media_ids = '['
  local media_types = '['

  for i = 0, num_user_mentions, 1 do
    local user_mention_id
    while (true) do
      user_mention_id = math.random(0, max_user_index - 1)
      if user_index ~= user_mention_id then
        break
      end
    end
    text = text .. " @username_" .. tostring(user_mention_id)
  end

  for i = 0, num_urls, 1 do
    text = text .. " http://" .. stringRandom(64)
  end

  for i = 0, num_media, 1 do
    local media_id = decRandom(18)
    media_ids = media_ids .. "\"" .. media_id .. "\","
    media_types = media_types .. "\"png\","
  end

  media_ids = media_ids:sub(1, #media_ids - 1) .. "]"
  media_types = media_types:sub(1, #media_types - 1) .. "]"

  local method = "POST"
  local path = "/wrk2-api/post/compose"
  local headers = {}
  local body
  headers["Content-Type"] = "application/x-www-form-urlencoded"
  if num_media then
    body   = "username=" .. username .. "&user_id=" .. user_id ..
        "&text=" .. text .. "&media_ids=" .. media_ids ..
        "&media_types=" .. media_types .. "&post_type=0"
  else
    body   = "username=" .. username .. "&user_id=" .. user_id ..
        "&text=" .. text .. "&media_ids=" .. "&post_type=0"
  end

  -- enqueue start time and key (path, method)
  local key = string.format("('%s','%s')", current_path, current_method)
  qpush({ t = socket.gettime(), key = key })

  return wrk.format(method, path, headers, body)
end

response = function(status, headers, body)
  local now = socket.gettime()
  local item = qpop()
  if item then
    local duration_ms = (now - item.t) * 1000

    -- per-interval
    request_counts[item.key] = (request_counts[item.key] or 0) + 1
    latency_buckets[item.key] = latency_buckets[item.key] or {}
    table.insert(latency_buckets[item.key], duration_ms)

    -- cumulative (for running percentiles)
    cumulative_latency_buckets[item.key] = cumulative_latency_buckets[item.key] or {}
    table.insert(cumulative_latency_buckets[item.key], duration_ms)
    table.insert(cumulative_all_latencies, duration_ms)
  end

  if now - last_send_time >= send_interval then
    flush_rps_file()
    last_send_time = now
  end
end

done = function(summary, latency, requests)
  flush_rps_file()
end
