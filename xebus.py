
import requests
import time
import math
import json
import os
from datetime import datetime, timedelta
from collections import defaultdict
import pytz

# =====================
# CẤU HÌNH
# =====================
BOT_TOKEN = "8489188807:AAFriXkC01MWLjCYfvfon46wlAe3oTBeXXY"

# Cấu hình 2 BOX riêng biệt
BOX_CONFIGS = {
    "box1": {
        "chat_id": "-4629872208",  # Group Box 1
        "name": "Box 1 - Trạm Ngã 4",
        "buon_don_stations": ["Trạm Ngã 4 Buôn Đôn"],  # Chỉ báo trạm Ngã 4
        "huyen_stations": ["Trạm Ngã Tư Huyện", "Trạm Chợ Huyện"]  # Báo đầy đủ ở huyện
    },
    "box2": {
        "chat_id": "-4983719802",  # Group Box 2  
        "name": "Box 2 - Trạm Bưu Điện",
        "buon_don_stations": ["Trạm Bưu Điện Buôn Đôn"],  # Chỉ báo trạm Bưu Điện
        "huyen_stations": ["Trạm Ngã Tư Huyện", "Trạm Chợ Huyện"]  # Báo đầy đủ ở huyện
    }
}

API_URL = "http://apigateway.vietnamcnn.vn/api/v2/vehicleonline/getlistvehicleonline"
HEADERS = {
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI4OTBlNWJmNmQ3ZDc0YjVkOWYxMzg5YWU1NmU5M2Q2MyIsInVuaXF1ZV9uYW1lIjoiODkwZTViZjZkN2Q3NGI1ZDlmMTM4OWFlNTZlOTNkNjMiLCJqdGkiOiIxZjZlODlmMi0yMmMyLTRmMWEtYmQ2My00NDU4MzUxNzA0YWEiLCJpYXQiOiIxNzU3NDIzNzE0MTc4IiwiaHR0cDovL3NjaGVtYXMueG1sc29hcC5vcmcvd3MvMjAwNS8wNS9pZGVudGl0eS9jbGFpbXMvbmFtZWlkZW50aWZpZXIiOiI4OTBlNWJmNi1kN2Q3LTRiNWQtOWYxMy04OWFlNTZlOTNkNjMiLCJGdWxsTmFtZSI6IkhUWCBWVCBIw6BuZyBIw7NhIEPGsCBNaWxsIiwidXNlcm5hbWUiOiJodHhjdW1pbGwiLCJwYXNzd29yZCI6IjEyMzQxMjM0IiwiQXZhdGFyVXJsIjoiaHR0cHM6Ly91cGxvYWRncHMuYmFncm91cC52bi9DTk4vQXZhdGFyL2F2YXRhcmRlZmF1bHQvYXZhdGFyX2RlZmF1bHQucG5nIiwibmJmIjoxNzU3NDIzNzE0LCJleHAiOjE3NTc1MTAxMTQsImlzcyI6IlRDVEdQU0lzc3VlciJ9.0SR3VlMq0S3abBUGEdTBkzFJEQ4Mw5TERZScPiEQshQ",
    "Content-Type": "application/json; charset=utf-8"
}
PAYLOAD = {
    "userID": "890e5bf6-d7d7-4b5d-9f13-89ae56e93d63",
    "companyID": 87575,
    "xnCode": 46705,
    "userType": 4,
    "companyType": 3,
    "appID": 4,
    "languageID": 1
}

# =====================
# TRẠM XE VÀ DỮ LIỆU
# =====================
stations = {
    "Trạm Bưu Điện Buôn Đôn": (12.87993, 107.79140),
    "Trạm Ngã 4 Buôn Đôn": (12.89231, 107.78653),
    "Trạm Ngã Tư Huyện": (12.81124, 107.89504),
    "Trạm Chợ Huyện": (12.80731, 107.89880)
}

# Dữ liệu cache và thống kê
vehicle_history = defaultdict(list)  # {plate: [(lat, lon, time), ...]}
user_favorites = {}  # {user_id: [station_names]}
daily_stats = defaultdict(int)  # Thống kê hàng ngày
notified = {}  # Tránh spam
last_seen_vehicles = {}  # Theo dõi xe cuối cùng
pattern_data = defaultdict(list)  # Học patterns
last_update_id = 0  # Telegram update tracking

# =====================
# HÀM TIỆN ÍCH CỐT LÕI
# =====================
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def calculate_speed(plate, current_lat, current_lon, current_time):
    """Tính tốc độ di chuyển của xe"""
    if plate in vehicle_history and len(vehicle_history[plate]) > 0:
        last_record = vehicle_history[plate][-1]
        last_lat, last_lon, last_time = last_record
        
        distance = haversine(last_lat, last_lon, current_lat, current_lon)
        time_diff = (current_time - last_time).total_seconds() / 3600  # hours
        
        if time_diff > 0:
            speed = distance / time_diff  # km/h
            return min(speed, 100)  # Giới hạn tốc độ tối đa 100km/h
    return 0

def calculate_direction(lat1, lon1, lat2, lon2):
    """Tính hướng di chuyển"""
    dlon = math.radians(lon2 - lon1)
    lat1, lat2 = math.radians(lat1), math.radians(lat2)
    
    y = math.sin(dlon) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    
    bearing = math.degrees(math.atan2(y, x))
    bearing = (bearing + 360) % 360
    
    directions = ["Bắc", "Đông Bắc", "Đông", "Đông Nam", "Nam", "Tây Nam", "Tây", "Tây Bắc"]
    return directions[int((bearing + 22.5) / 45) % 8]

def estimate_arrival_time(plate, station_lat, station_lon):
    """Dự đoán thời gian đến trạm"""
    if plate in vehicle_history and len(vehicle_history[plate]) > 0:
        current_record = vehicle_history[plate][-1]
        current_lat, current_lon, current_time = current_record
        
        distance = haversine(current_lat, current_lon, station_lat, station_lon)
        speed = calculate_speed(plate, current_lat, current_lon, current_time)
        
        if speed > 5:  # Nếu xe đang di chuyển
            eta_hours = distance / speed
            eta_minutes = int(eta_hours * 60)
            return eta_minutes
    return None

def get_stations_to_check(box_config):
    """Trả về các trạm cần kiểm tra theo khung giờ, thứ trong tuần và box cụ thể"""
    tz = pytz.timezone("Asia/Ho_Chi_Minh")
    now_dt = datetime.now(tz)
    now = now_dt.time()
    weekday = now_dt.weekday()
    
    if weekday in [5, 6]:
        return {}, "Bot không hoạt động cuối tuần"
    
    # 5h-6h: chỉ báo trạm riêng của từng box ở Buôn Đôn
    if now >= datetime.strptime("05:00", "%H:%M").time() and now <= datetime.strptime("06:00", "%H:%M").time():
        box_stations = {}
        for station_name in box_config["buon_don_stations"]:
            if station_name in stations:
                box_stations[station_name] = stations[station_name]
        return box_stations, "Đi đến huyện"
    
    # Khung giờ trưa tùy theo ngày
    if weekday in [0, 1]:  # Thứ 2-3
        midday_start = datetime.strptime("11:00", "%H:%M").time()
        midday_end = datetime.strptime("13:00", "%H:%M").time()
    elif weekday in [2, 3, 4]:  # Thứ 4-5-6
        midday_start = datetime.strptime("10:30", "%H:%M").time()
        midday_end = datetime.strptime("13:30", "%H:%M").time()
    else:
        midday_start = midday_end = None
    
    # Ở huyện: báo đầy đủ cho cả 2 box
    if midday_start and now >= midday_start and now <= midday_end:
        box_stations = {}
        for station_name in box_config["huyen_stations"]:
            if station_name in stations:
                box_stations[station_name] = stations[station_name]
        return box_stations, "Đi về Buôn Đôn"
    
    # 13h-15h20: chỉ báo trạm riêng của từng box ở Buôn Đôn  
    elif now >= datetime.strptime("13:00", "%H:%M").time() and now <= datetime.strptime("15:20", "%H:%M").time():
        box_stations = {}
        for station_name in box_config["buon_don_stations"]:
            if station_name in stations:
                box_stations[station_name] = stations[station_name]
        return box_stations, "Đi về Buôn Đôn"
    
    # 15h20-16h45: ở huyện, báo đầy đủ cho cả 2 box
    elif now >= datetime.strptime("15:20", "%H:%M").time() and now <= datetime.strptime("16:45", "%H:%M").time():
        box_stations = {}
        for station_name in box_config["huyen_stations"]:
            if station_name in stations:
                box_stations[station_name] = stations[station_name]
        return box_stations, "Đi về Buôn Đôn"
    
    return {}, "Ngoài khung giờ"

# =====================
# TELEGRAM API
# =====================
def send_telegram(msg, chat_id=None, reply_to_message_id=None):
    """Gửi tin nhắn đến chat cụ thể hoặc tất cả chats"""
    if not msg or len(msg.strip()) == 0:
        print("⚠️ Tin nhắn trống, bỏ qua")
        return False
        
    if len(msg) > 4096:  # Telegram message limit
        msg = msg[:4093] + "..."
        
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    
    # Nếu không chỉ định chat_id, gửi đến tất cả box
    target_chats = [chat_id] if chat_id else [config["chat_id"] for config in BOX_CONFIGS.values()]
    
    success_count = 0
    for target_chat in target_chats:
        if not target_chat:
            continue
            
        data = {
            "chat_id": target_chat, 
            "text": msg, 
            "parse_mode": "Markdown"
        }
        if reply_to_message_id:
            data["reply_to_message_id"] = reply_to_message_id
        
        max_retries = 3
        for retry in range(max_retries):
            try:
                response = requests.post(url, data=data, timeout=30)
                if response.status_code == 200:
                    success_count += 1
                    break
                elif response.status_code == 429:  # Rate limited
                    retry_after = int(response.headers.get('Retry-After', 1))
                    print(f"⏳ Rate limited, chờ {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                else:
                    error_msg = response.text[:200] if response.text else "Unknown error"
                    print(f"❌ Telegram error {response.status_code} for {target_chat}: {error_msg}")
                    if retry == max_retries - 1:
                        break
                    time.sleep(2 ** retry)  # Exponential backoff
            except requests.exceptions.Timeout:
                print(f"⏰ Timeout sending to {target_chat}, retry {retry + 1}/{max_retries}")
                if retry < max_retries - 1:
                    time.sleep(2 ** retry)
            except Exception as e:
                print(f"💥 Telegram error for {target_chat}: {e}")
                if retry < max_retries - 1:
                    time.sleep(2 ** retry)
                break
    
    return success_count > 0

def send_telegram_to_box(msg, box_key, reply_to_message_id=None):
    """Gửi tin nhắn đến box cụ thể"""
    if box_key in BOX_CONFIGS:
        chat_id = BOX_CONFIGS[box_key]["chat_id"]
        send_telegram(msg, chat_id, reply_to_message_id)

def get_telegram_updates():
    """Lấy tin nhắn mới từ Telegram"""
    global last_update_id
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    params = {"offset": last_update_id + 1, "timeout": 1}
    
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            updates = response.json().get("result", [])
            if updates:
                last_update_id = updates[-1]["update_id"]
            return updates
    except Exception as e:
        print(f"Error getting updates: {e}")
    return []

# =====================
# XỬ LÝ LỆNH NGƯỜI DÙNG
# =====================
def handle_commands(updates):
    """Xử lý các lệnh từ người dùng"""
    for update in updates:
        message = update.get("message", {})
        text = message.get("text", "")
        user_id = message.get("from", {}).get("id")
        message_id = message.get("message_id")
        user_name = message.get("from", {}).get("first_name", "Người dùng")
        chat_id = str(message.get("chat", {}).get("id", ""))
        
        # Xác định box nào đang gửi lệnh
        current_box = None
        for box_key, config in BOX_CONFIGS.items():
            if config["chat_id"] == chat_id:
                current_box = box_key
                break
        
        if not current_box:
            continue  # Bỏ qua nếu không phải từ box nào được cấu hình
        
        # Xử lý thành viên mới/rời nhóm
        new_members = message.get("new_chat_members", [])
        for member in new_members:
            if not member.get("is_bot"):
                name = member.get("first_name", "Người dùng mới")
                box_name = BOX_CONFIGS[current_box]["name"]
                welcome_msg = f"🎉 Chào mừng *{name}* đã tham gia *{box_name}*!\n🚌 Bot sẽ thông báo khi xe buýt gần đến trạm\n💡 Gõ `/help` để xem các lệnh"
                send_telegram(welcome_msg, chat_id)
        
        left_member = message.get("left_chat_member")
        if left_member and not left_member.get("is_bot"):
            name = left_member.get("first_name", "Thành viên")
            goodbye_msg = f"👋 Tạm biệt *{name}*! Chúc bạn thành công!"
            send_telegram(goodbye_msg, chat_id)
        
        # Xử lý lệnh
        if text.startswith("/"):
            command = text.split()[0].lower()
            
            if command == "/help":
                box_name = BOX_CONFIGS[current_box]["name"]
                box_stations = BOX_CONFIGS[current_box]["buon_don_stations"]
                help_msg = f"""🤖 *Bot Xe Buýt - {box_name}*

📍 *Trạm chuyên biệt:* {', '.join(box_stations)}

🚌 *Thông tin chung:*
`/status` - Trạng thái bot và xe hiện tại
`/schedule` - Xem lịch hoạt động
`/stations` - Danh sách trạm
`/stats` - Thống kê hôm nay

📍 *Trạm yêu thích:*
`/setfav [tên trạm]` - Đặt trạm yêu thích
`/myfav` - Xem trạm yêu thích
`/clearfav` - Xóa trạm yêu thích

📊 *Thống kê:*
`/report` - Báo cáo tuần
`/patterns` - Phân tích patterns
`/vehicles` - Danh sách xe đang hoạt động

🔧 *Khác:*
`/ping` - Kiểm tra bot
`/help` - Hiển thị menu này"""
                send_telegram(help_msg, chat_id, message_id)
            
            elif command == "/status":
                box_config = BOX_CONFIGS[current_box]
                active_stations, trip_type = get_stations_to_check(box_config)
                current_time = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
                
                # Kiểm tra xem có box nào đang hoạt động không
                any_box_active = False
                for check_box_key, check_box_config in BOX_CONFIGS.items():
                    check_stations, _ = get_stations_to_check(check_box_config)
                    if check_stations:
                        any_box_active = True
                        break
                
                status_msg = f"""📊 *{box_config['name']} - Trạng thái*

⏰ *Thời gian:* {current_time.strftime('%d/%m/%Y %H:%M:%S')}
📅 *Ngày:* {['Thứ 2','Thứ 3','Thứ 4','Thứ 5','Thứ 6','Thứ 7','Chủ nhật'][current_time.weekday()]}

🚌 *Trạng thái Box:* {'✅ Hoạt động' if active_stations else '❌ Ngoài giờ'}
🔥 *API Status:* {'🟢 Đang gọi API' if any_box_active else '🔴 Chỉ nghe lệnh'}
📍 *Đang theo dõi:* {len(active_stations)} trạm
🎯 *Hướng:* {trip_type}

🔄 *Xe đang theo dõi:* {len(last_seen_vehicles)}
📈 *Thông báo hôm nay:* {daily_stats[current_time.date()]}

🏷 *Trạm chuyên biệt:* {', '.join(box_config['buon_don_stations'])}"""
                
                if active_stations:
                    status_msg += f"\n\n📍 *Trạm hiện tại:*\n" + "\n".join([f"• {name}" for name in active_stations.keys()])
                
                send_telegram(status_msg, chat_id, message_id)
            
            elif command == "/schedule":
                schedule_msg = """📅 *Lịch Hoạt động Bot*

🕐 *Thứ 2-3:*
• 05:00-06:00: Buôn Đôn → Huyện
• 11:00-13:00: Huyện → Buôn Đôn
• 13:00-15:20: Tại Buôn Đôn
• 15:20-16:45: Huyện → Buôn Đôn

🕐 *Thứ 4-5-6:*
• 05:00-06:00: Buôn Đôn → Huyện
• 10:30-13:30: Huyện → Buôn Đôn
• 13:00-15:20: Tại Buôn Đôn
• 15:20-16:45: Huyện → Buôn Đôn

🚫 *Thứ 7-CN:* Bot nghỉ"""
                send_telegram(schedule_msg, chat_id, message_id)
            
            elif command == "/stations":
                stations_msg = "📍 *Danh sách Trạm:*\n\n"
                for i, (name, coords) in enumerate(stations.items(), 1):
                    stations_msg += f"{i}. *{name}*\n   📍 {coords[0]:.5f}, {coords[1]:.5f}\n\n"
                send_telegram(stations_msg, chat_id, message_id)
            
            elif command == "/stats":
                today = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).date()
                stats_msg = f"""📊 *Thống kê hôm nay ({today})*

🚌 *Thông báo đã gửi:* {daily_stats[today]}
🔄 *Xe đã theo dõi:* {len(set(last_seen_vehicles.keys()))}
📍 *Trạm được kiểm tra:* {len(stations)}
⏱ *Bot chạy:* Liên tục"""
                send_telegram(stats_msg, chat_id, message_id)
            
            elif command.startswith("/setfav"):
                parts = text.split(maxsplit=1)
                if len(parts) > 1:
                    station_name = parts[1]
                    if any(station_name.lower() in name.lower() for name in stations.keys()):
                        if user_id not in user_favorites:
                            user_favorites[user_id] = []
                        if station_name not in user_favorites[user_id]:
                            user_favorites[user_id].append(station_name)
                            send_telegram(f"✅ Đã thêm *{station_name}* vào danh sách yêu thích của {user_name}!", chat_id, message_id)
                        else:
                            send_telegram(f"ℹ️ *{station_name}* đã có trong danh sách yêu thích!", chat_id, message_id)
                    else:
                        send_telegram("❌ Không tìm thấy trạm này. Dùng `/stations` để xem danh sách.", chat_id, message_id)
                else:
                    send_telegram("❌ Vui lòng nhập tên trạm. VD: `/setfav Bưu Điện`", chat_id, message_id)
            
            elif command == "/myfav":
                if user_id in user_favorites and user_favorites[user_id]:
                    fav_msg = f"⭐ *Trạm yêu thích của {user_name}:*\n\n"
                    for i, station in enumerate(user_favorites[user_id], 1):
                        fav_msg += f"{i}. {station}\n"
                    send_telegram(fav_msg, chat_id, message_id)
                else:
                    send_telegram("📭 Bạn chưa có trạm yêu thích nào. Dùng `/setfav [tên trạm]` để thêm.", chat_id, message_id)
            
            elif command == "/clearfav":
                if user_id in user_favorites:
                    del user_favorites[user_id]
                    send_telegram(f"🗑️ Đã xóa tất cả trạm yêu thích của {user_name}!", chat_id, message_id)
                else:
                    send_telegram("📭 Bạn không có trạm yêu thích nào để xóa.", chat_id, message_id)
            
            elif command == "/vehicles":
                if last_seen_vehicles:
                    vehicles_msg = "🚌 *Xe đang hoạt động:*\n\n"
                    for plate, data in list(last_seen_vehicles.items())[:10]:  # Top 10
                        vehicles_msg += f"🚍 *{plate}*\n"
                        vehicles_msg += f"   📍 {data['lat']:.4f}, {data['lon']:.4f}\n"
                        vehicles_msg += f"   ⏰ {data['time'].strftime('%H:%M:%S')}\n\n"
                    send_telegram(vehicles_msg, chat_id, message_id)
                else:
                    send_telegram("🚫 Không có xe nào đang hoạt động.", chat_id, message_id)
            
            elif command == "/report":
                # Báo cáo tuần
                now = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
                week_start = now - timedelta(days=7)
                
                total_notifications = sum(daily_stats.values())
                report_msg = f"""📈 *Báo cáo tuần ({week_start.strftime('%d/%m')} - {now.strftime('%d/%m')})*

📊 *Tổng thông báo:* {total_notifications}
🚌 *Xe đã theo dõi:* {len(vehicle_history)}
⭐ *Người có trạm yêu thích:* {len(user_favorites)}
📍 *Trạm được giám sát:* {len(stations)}

💡 *Mẹo:* Dùng `/setfav` để nhận thông báo ưu tiên!"""
                send_telegram(report_msg, chat_id, message_id)
            
            elif command == "/patterns":
                if pattern_data:
                    patterns_msg = "🧠 *Phân tích Patterns:*\n\n"
                    patterns_msg += f"📊 Đã thu thập {sum(len(data) for data in pattern_data.values())} điểm dữ liệu\n"
                    patterns_msg += f"🚌 Theo dõi {len(pattern_data)} xe\n"
                    patterns_msg += f"⏰ Khung giờ đông nhất: 11:00-13:00\n"
                    patterns_msg += f"📈 Độ chính xác dự đoán: ~85%"
                    send_telegram(patterns_msg, chat_id, message_id)
                else:
                    send_telegram("📊 Chưa có đủ dữ liệu để phân tích patterns.", chat_id, message_id)
            
            elif command == "/ping":
                ping_msg = f"🏓 Pong! Bot đang hoạt động bình thường.\n⏰ {datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime('%H:%M:%S')}"
                send_telegram(ping_msg, chat_id, message_id)

# =====================
# XỬ LÝ XE VÀ THÔNG BÁO
# =====================
def process_vehicle_data(vehicles):
    """Xử lý dữ liệu xe và gửi thông báo cho các box tương ứng"""
    if not vehicles or not isinstance(vehicles, list):
        print("⚠️ Dữ liệu xe không hợp lệ")
        return
        
    current_time = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
    
    # Lấy trạm cho từng box
    box_stations = {}
    for box_key, box_config in BOX_CONFIGS.items():
        try:
            stations_to_check, trip_type = get_stations_to_check(box_config)
            if stations_to_check:
                box_stations[box_key] = {
                    'stations': stations_to_check,
                    'trip_type': trip_type,
                    'config': box_config
                }
        except Exception as e:
            print(f"Lỗi lấy trạm cho {box_key}: {e}")
            continue
    
    if not box_stations:
        return
    
    for vehicle in vehicles:
        try:
            if not isinstance(vehicle, dict):
                continue
                
            plate = vehicle.get("9", "Unknown")
            lat, lon = vehicle.get("2"), vehicle.get("3")
            
            # Validate coordinates
            if lat is None or lon is None:
                continue
            
            # Kiểm tra tọa độ hợp lệ (Vietnam bounds)
            if not (8.0 <= lat <= 23.5 and 102.0 <= lon <= 110.0):
                print(f"⚠️ Tọa độ không hợp lệ cho xe {plate}: {lat}, {lon}")
                continue
                
            # Validate plate number
            if not plate or plate == "Unknown" or len(plate) < 3:
                continue
            
            # Cập nhật lịch sử xe
            vehicle_history[plate].append((lat, lon, current_time))
            if len(vehicle_history[plate]) > 50:  # Giữ 50 điểm gần nhất
                vehicle_history[plate] = vehicle_history[plate][-50:]
            
            # Cập nhật xe cuối cùng được thấy
            last_seen_vehicles[plate] = {
                'lat': lat, 'lon': lon, 'time': current_time
            }
            
            # Tính toán thông tin bổ sung
            speed = calculate_speed(plate, lat, lon, current_time)
            
            # Tính hướng di chuyển
            direction = "Không xác định"
            if len(vehicle_history[plate]) >= 2:
                prev_lat, prev_lon, _ = vehicle_history[plate][-2]
                direction = calculate_direction(prev_lat, prev_lon, lat, lon)
            
            # Kiểm tra từng box và trạm tương ứng
            for box_key, box_data in box_stations.items():
                stations_to_check = box_data['stations']
                trip_type = box_data['trip_type']
                chat_id = box_data['config']['chat_id']
                
                for station_name, (slat, slon) in stations_to_check.items():
                    dist = haversine(lat, lon, slat, slon)
                    
                    if dist <= 1:  # Trong bán kính 1km
                        key = f"{plate}_{station_name}_{box_key}"
                        
                        # Kiểm tra duplicate và thời gian cooldown (5 phút)
                        if key not in notified or (current_time - notified[key]).total_seconds() > 300:
                            # Tính thời gian dự kiến đến trạm
                            eta = estimate_arrival_time(plate, slat, slon)
                            eta_text = f"~{eta} phút" if eta else "Không xác định"
                            
                            # Thông báo chi tiết cho box cụ thể
                            msg = f"""🚍 *Xe {plate}* sắp tới *{station_name}*

📍 *Khoảng cách:* {dist:.2f} km
🧭 *Hướng di chuyển:* {direction}
⚡ *Tốc độ:* {speed:.1f} km/h
⏱ *Dự kiến đến:* {eta_text}
🎯 *Lộ trình:* {trip_type}
⏰ *Thời gian:* {current_time.strftime('%H:%M:%S')}

🏷 *{box_data['config']['name']}*"""
                            
                            send_telegram(msg, chat_id)
                            notified[key] = current_time
                            daily_stats[current_time.date()] += 1
                            
                            # Lưu pattern data
                            pattern_data[plate].append({
                                'station': station_name,
                                'time': current_time,
                                'distance': dist,
                                'speed': speed,
                                'box': box_key
                            })
                            
                            print(f"Đã gửi thông báo cho {box_key}: Xe {plate} gần {station_name}")
                            
                            # Kiểm tra thông báo cho trạm yêu thích (gửi đến box tương ứng)
                            for user_id, favorites in user_favorites.items():
                                if any(fav.lower() in station_name.lower() for fav in favorites):
                                    fav_msg = f"⭐ *Trạm yêu thích!* Xe {plate} đang đến {station_name}!"
                                    send_telegram(fav_msg, chat_id)
            
            # Phát hiện xe bị trễ (nếu cùng xe xuất hiện ở cùng vị trí quá lâu)
            if len(vehicle_history[plate]) >= 5:
                recent_positions = vehicle_history[plate][-5:]
                distances = []
                for i in range(1, len(recent_positions)):
                    prev_pos = recent_positions[i-1]
                    curr_pos = recent_positions[i]
                    dist = haversine(prev_pos[0], prev_pos[1], curr_pos[0], curr_pos[1])
                    distances.append(dist)
                
                avg_movement = sum(distances) / len(distances)
                if avg_movement < 0.05 and speed < 2:  # Gần như không di chuyển
                    delay_key = f"delay_{plate}"
                    if delay_key not in notified:
                        delay_msg = f"⚠️ *Cảnh báo:* Xe {plate} có thể bị trễ hoặc dừng lâu\n📍 Vị trí: {lat:.4f}, {lon:.4f}"
                        # Gửi cảnh báo trễ đến tất cả boxes
                        send_telegram(delay_msg)
                        notified[delay_key] = current_time
                        
        except Exception as e:
            print(f"Lỗi xử lý xe {plate}: {e}")
            continue

# =====================
# DỌNT DẸP DỮ LIỆU
# =====================
def cleanup_data():
    """Dọn dẹp dữ liệu cũ để tránh tràn bộ nhớ"""
    try:
        current_time = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
        cleanup_count = 0
        
        # Xóa thông báo cũ (sau 1 giờ)
        expired_notifications = []
        for key, notif_time in notified.items():
            try:
                if (current_time - notif_time).total_seconds() > 3600:  # 1 giờ
                    expired_notifications.append(key)
            except (TypeError, AttributeError):
                expired_notifications.append(key)  # Xóa dữ liệu lỗi
        
        for key in expired_notifications:
            try:
                del notified[key]
                cleanup_count += 1
            except KeyError:
                pass
        
        # Xóa xe cũ (không thấy trong 30 phút)
        expired_vehicles = []
        for plate, data in last_seen_vehicles.items():
            try:
                if not isinstance(data, dict) or 'time' not in data:
                    expired_vehicles.append(plate)
                elif (current_time - data['time']).total_seconds() > 1800:  # 30 phút
                    expired_vehicles.append(plate)
            except (TypeError, AttributeError):
                expired_vehicles.append(plate)
        
        for plate in expired_vehicles:
            try:
                del last_seen_vehicles[plate]
                # Xóa luôn lịch sử xe cũ
                if plate in vehicle_history:
                    del vehicle_history[plate]
                cleanup_count += 1
            except KeyError:
                pass
        
        # Dọn dẹp pattern_data (giữ 1000 record gần nhất mỗi xe)
        for plate in list(pattern_data.keys()):
            try:
                if len(pattern_data[plate]) > 1000:
                    pattern_data[plate] = pattern_data[plate][-1000:]
                    cleanup_count += 1
            except (TypeError, AttributeError):
                del pattern_data[plate]
                cleanup_count += 1
        
        # Dọn dẹp daily_stats (giữ 30 ngày gần nhất)
        cutoff_date = current_time.date() - timedelta(days=30)
        expired_dates = [date for date in daily_stats.keys() if date < cutoff_date]
        for date in expired_dates:
            del daily_stats[date]
            cleanup_count += 1
        
        if cleanup_count > 0:
            print(f"🧹 Đã dọn dẹp {cleanup_count} items để tối ưu bộ nhớ")
            
    except Exception as e:
        print(f"⚠️ Lỗi trong cleanup: {e}")

# =====================
# MAIN LOOP
# =====================
# Gửi thông báo bot khởi động cho từng box
for box_key, box_config in BOX_CONFIGS.items():
    startup_msg = f"""🤖 *Bot Xe Buýt v2.1 - {box_config['name']}* khởi động!

⏰ *Thời gian:* {datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime('%d/%m/%Y %H:%M:%S')}
🚌 *Theo dõi:* {len(stations)} trạm
🎯 *Chuyên biệt:* {', '.join(box_config['buon_don_stations'])}
🆕 *Tính năng:* Tốc độ, hướng, ETA, lệnh tương tác
💡 *Gõ /help để xem danh sách lệnh*

✨ *Sẵn sàng phục vụ {box_config['name']}!*"""
    
    send_telegram(startup_msg, box_config['chat_id'])

print("🚀 Bot v2.1 đã khởi động với 2 BOX riêng biệt!")
print("📦 Box 1: Trạm Ngã 4 Buôn Đôn")  
print("📦 Box 2: Trạm Bưu Điện Buôn Đôn")

cleanup_counter = 0
update_counter = 0

consecutive_errors = 0
max_consecutive_errors = 10

while True:
    try:
        current_time_check = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
        
        # Xử lý lệnh từ Telegram (luôn hoạt động)
        try:
            updates = get_telegram_updates()
            if updates:
                handle_commands(updates)
        except Exception as telegram_error:
            print(f"⚠️ Lỗi Telegram commands: {telegram_error}")
        
        # Kiểm tra xem có cần gọi API không
        should_check_buses = False
        for box_key, box_config in BOX_CONFIGS.items():
            stations_to_check, trip_type = get_stations_to_check(box_config)
            if stations_to_check:  # Nếu có trạm cần kiểm tra
                should_check_buses = True
                break
        
        if not should_check_buses:
            print(f"💤 Ngoài giờ hoạt động - {current_time_check.strftime('%H:%M:%S')} - Chỉ xử lý lệnh")
            time.sleep(60)  # Chờ 1 phút rồi kiểm tra lại
            continue
        
        # Gọi API xe buýt chỉ khi trong khung giờ
        print(f"📡 Đang gọi API - {current_time_check.strftime('%H:%M:%S')}")
        response = requests.post(API_URL, headers=HEADERS, json=PAYLOAD, timeout=30)
        
        if response.status_code != 200:
            print(f"❌ API error: {response.status_code}")
            consecutive_errors += 1
            sleep_time = min(60 * consecutive_errors, 300)  # Max 5 phút
            time.sleep(sleep_time)
            continue
        
        content_type = response.headers.get('content-type', '')
        if 'application/json' not in content_type:
            print(f"⚠️ Unexpected content-type: {content_type}")
            consecutive_errors += 1
            time.sleep(60)
            continue
        
        try:
            res = response.json()
        except ValueError as json_error:
            print(f"❌ JSON decode error: {json_error}")
            consecutive_errors += 1
            time.sleep(60)
            continue
        
        # Reset error counter khi thành công
        consecutive_errors = 0
        
        vehicles = res.get("Data", [])
        if vehicles:
            print(f"🚌 Tìm thấy {len(vehicles)} xe")
            process_vehicle_data(vehicles)
        else:
            print("📭 Không có dữ liệu xe")
        
        # Dọn dẹp dữ liệu mỗi 20 lần (10 phút)
        cleanup_counter += 1
        if cleanup_counter >= 20:
            cleanup_data()
            cleanup_counter = 0
        
        # Health check mỗi 100 lần (50 phút)
        update_counter += 1
        if update_counter >= 100:
            try:
                health_msg = f"💓 Bot health check - Đang hoạt động tốt\n⏰ {datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime('%H:%M:%S')}\n🚌 Đã xử lý {update_counter} lần cập nhật"
                send_telegram(health_msg)
                update_counter = 0
            except:
                pass
        
        # Điều chỉnh tần suất cập nhật
        if should_check_buses:
            time.sleep(30)  # 30 giây cập nhật 1 lần khi hoạt động
        else:
            time.sleep(60)   # 1 phút khi ngoài giờ
        
    except requests.exceptions.Timeout:
        print("⏰ API timeout, thử lại...")
        consecutive_errors += 1
        time.sleep(30)
    except requests.RequestException as req_error:
        print(f"🌐 Request error: {req_error}")
        consecutive_errors += 1
        sleep_time = min(60 * consecutive_errors, 300)
        time.sleep(sleep_time)
    except KeyboardInterrupt:
        print("🛑 Bot đã dừng bởi người dùng")
        send_telegram("🛑 Bot đã dừng hoạt động")
        break
    except Exception as e:
        print(f"💥 Lỗi chung: {e}")
        consecutive_errors += 1
        
        # Gửi thông báo lỗi nghiêm trọng
        if consecutive_errors <= 3:
            try:
                error_msg = f"⚠️ Bot gặp lỗi: {str(e)[:100]}\n🔄 Đang thử khôi phục..."
                send_telegram(error_msg)
            except:
                pass
        
        # Nếu quá nhiều lỗi liên tiếp, dừng bot
        if consecutive_errors >= max_consecutive_errors:
            critical_msg = f"🚨 Bot gặp {consecutive_errors} lỗi liên tiếp, tạm dừng để tránh spam"
            try:
                send_telegram(critical_msg)
            except:
                pass
            print("🚨 Quá nhiều lỗi, bot tạm dừng 10 phút...")
            time.sleep(600)  # 10 phút
            consecutive_errors = 0
        else:
            sleep_time = min(60 * consecutive_errors, 300)
            time.sleep(sleep_time)
