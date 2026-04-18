"""
aligo_sms.py
알리고 문자 발송 API 연동

API 문서: https://smartsms.aligo.in/admin/api/spec.html
"""

import json
import logging
import os
import requests

logger = logging.getLogger(__name__)

API_BASE = "https://apis.aligo.in"

# 설정
_config = {
    "api_key": "",
    "user_id": "",
    "sender": "",   # 발신번호 (사전 등록 필요)
}


def load_config():
    """설정 파일에서 로드"""
    config_path = os.path.join(os.path.dirname(__file__), "aligo_config.json")
    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                _config.update(json.load(f))
                return True
        except Exception:
            pass
    return False


def save_config(api_key, user_id, sender):
    """설정 저장"""
    config_path = os.path.join(os.path.dirname(__file__), "aligo_config.json")
    data = {"api_key": api_key, "user_id": user_id, "sender": sender}
    with open(config_path, "w") as f:
        json.dump(data, f, indent=2)
    _config.update(data)


def send_sms(receiver, msg, title="", msg_type="SMS"):
    """문자 발송

    Args:
        receiver: 수신번호 (쉼표로 여러명 가능, 최대 1000명)
        msg: 메시지 내용
        title: 제목 (LMS/MMS만)
        msg_type: SMS(90바이트), LMS(2000바이트), MMS(이미지)

    Returns:
        {"ok": True/False, "result_code": N, "message": "...", "success_cnt": N}
    """
    if not _config["api_key"]:
        load_config()
    if not _config["api_key"] or not _config["user_id"] or not _config["sender"]:
        return {"ok": False, "message": "알리고 API 설정이 필요합니다 (API키, 유저ID, 발신번호)"}

    # 90바이트 초과 시 자동 LMS 전환
    if len(msg.encode("euc-kr", errors="replace")) > 90 and msg_type == "SMS":
        msg_type = "LMS"

    data = {
        "key": _config["api_key"],
        "user_id": _config["user_id"],
        "sender": _config["sender"],
        "receiver": receiver,
        "msg": msg,
        "msg_type": msg_type,
    }
    if title and msg_type in ("LMS", "MMS"):
        data["title"] = title

    try:
        resp = requests.post(f"{API_BASE}/send/", data=data, timeout=15)
        result = resp.json()
        ok = int(result.get("result_code", -1)) > 0
        logger.info(f"[알리고] {'성공' if ok else '실패'}: {receiver} → {result.get('message', '')}")
        return {"ok": ok, **result}
    except Exception as e:
        logger.error(f"[알리고] 발송 오류: {e}")
        return {"ok": False, "message": str(e)}


def send_bulk(receivers, msg, title="", msg_type="SMS"):
    """대량 발송 (각각 다른 번호에 같은 메시지)

    Args:
        receivers: 수신번호 리스트 ["010-1234-5678", "010-9876-5432"]
        msg: 메시지 내용

    Returns:
        {"ok": True, "success_cnt": N, "fail_cnt": N}
    """
    # 쉼표로 합쳐서 1000명씩 분할
    success = 0
    fail = 0
    for i in range(0, len(receivers), 1000):
        batch = receivers[i:i + 1000]
        receiver_str = ",".join(batch)
        result = send_sms(receiver_str, msg, title, msg_type)
        if result["ok"]:
            success += int(result.get("success_cnt", 0))
        else:
            fail += len(batch)
    return {"ok": success > 0, "success_cnt": success, "fail_cnt": fail}


def check_balance():
    """잔여 건수 확인"""
    if not _config["api_key"]:
        load_config()
    if not _config["api_key"]:
        return {"ok": False, "message": "API 키 미설정"}
    try:
        resp = requests.post(f"{API_BASE}/remain/", data={
            "key": _config["api_key"],
            "user_id": _config["user_id"],
        }, timeout=10)
        result = resp.json()
        return {
            "ok": int(result.get("result_code", -1)) > 0,
            "sms_cnt": result.get("SMS_CNT", 0),
            "lms_cnt": result.get("LMS_CNT", 0),
            "mms_cnt": result.get("MMS_CNT", 0),
        }
    except Exception as e:
        return {"ok": False, "message": str(e)}


# 주문 알림 템플릿
def send_order_notification(phone, order_number, status, product_name=""):
    """주문 상태 변경 알림"""
    status_msg = {
        "confirmed": f"[TheOne Vintage] 주문이 확인되었습니다.\n주문번호: {order_number}\n상품: {product_name}\n빠른 시일 내 처리하겠습니다.",
        "processing": f"[TheOne Vintage] 상품을 처리 중입니다.\n주문번호: {order_number}\n상품: {product_name}",
        "shipped": f"[TheOne Vintage] 상품이 발송되었습니다!\n주문번호: {order_number}\n상품: {product_name}\n배송 추적은 마이페이지에서 확인하세요.",
        "completed": f"[TheOne Vintage] 배송이 완료되었습니다.\n주문번호: {order_number}\n이용해 주셔서 감사합니다.",
    }
    msg = status_msg.get(status)
    if not msg:
        return {"ok": False, "message": f"알 수 없는 상태: {status}"}
    return send_sms(phone, msg, title="TheOne Vintage", msg_type="LMS")


if __name__ == "__main__":
    load_config()
    print("잔여 건수:", check_balance())
