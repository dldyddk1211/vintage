"""
data_manager.py
서버 데이터 경로 관리

구조:
  공유폴더(NAS): /Volumes/파일공유/00 이용아/thone/srv/data/vintage/
    → products.db, 스케줄, AI설정, 이미지 등 모든 데이터
  로컬:          기존 경로
    → users.db (회원/주문/장바구니 — 외부 접근 쇼핑몰 전용)
"""

import json
import logging
import os
import platform

logger = logging.getLogger(__name__)

PROJECT_NAME = "vintage"

# NAS 공유 폴더 경로 (OS별)
if platform.system() == "Darwin":
    NAS_SHARED_PATH = "/Volumes/파일공유/00 이용아/thone/srv/data/vintage"
else:
    NAS_SHARED_PATH = r"Z:\VOL1\파일공유\00 이용아\thone\srv\data\vintage"

# OS별 기본 경로 (로컬 — users.db 전용 폴백)
_home = os.path.expanduser("~")
_DEFAULT_PATHS = {
    "Darwin":  os.path.join(_home, "Documents", "theone", "srv", "data", PROJECT_NAME),
    "Windows": os.path.join(_home, "Documents", "theone", "srv", "data", PROJECT_NAME),
}

# 설정 파일 (프로젝트 루트에 저장)
_CONFIG_FILE = os.path.join(os.path.dirname(__file__), "data_path.json")

# 하위 디렉토리 구조
SUBDIRS = {
    "db":      "db",
    "outputs": "outputs",
    "logs":    "logs",
}

# 현재 설정값 (런타임)
_current_path = None


def _detect_os() -> str:
    """현재 OS 감지"""
    sys = platform.system()
    if sys == "Darwin":
        return "Darwin"
    return "Windows"


def _default_path() -> str:
    """OS에 맞는 기본 경로"""
    return _DEFAULT_PATHS.get(_detect_os(), _DEFAULT_PATHS["Windows"])


def _load_config() -> dict:
    """저장된 설정 로드"""
    if os.path.exists(_CONFIG_FILE):
        try:
            with open(_CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_config(cfg: dict):
    """설정 저장"""
    with open(_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def get_data_root() -> str:
    """현재 데이터 루트 경로 반환"""
    global _current_path
    if _current_path is None:
        cfg = _load_config()
        _current_path = cfg.get("data_root", _default_path())
    return _current_path


def set_data_root(path: str) -> bool:
    """데이터 루트 경로 변경 + 하위 디렉토리 생성"""
    global _current_path
    path = path.strip().rstrip("/\\")
    if not path:
        return False

    # 하위 디렉토리 생성 시도
    try:
        for sub in SUBDIRS.values():
            os.makedirs(os.path.join(path, sub), exist_ok=True)
    except Exception as e:
        logger.error(f"데이터 경로 생성 실패: {e}")
        return False

    _current_path = path
    _save_config({"data_root": path})
    logger.info(f"데이터 경로 변경: {path}")
    return True


def get_path(subdir: str) -> str:
    """하위 경로 반환 — 항상 로컬 (Mac/Windows 모두)
    NAS는 동기화 파일 복사용으로만 사용, DB를 직접 열지 않음
    """
    path = os.path.join(get_data_root(), SUBDIRS.get(subdir, subdir))
    os.makedirs(path, exist_ok=True)
    return path


def get_nas_path(subdir: str) -> str:
    """NAS 공유 폴더 경로 (파일 복사/동기화 전용 — DB 직접 열기 금지)"""
    path = os.path.join(NAS_SHARED_PATH, SUBDIRS.get(subdir, subdir))
    if os.path.isdir(NAS_SHARED_PATH):
        os.makedirs(path, exist_ok=True)
    return path


def get_local_path(subdir: str) -> str:
    """get_path와 동일 (하위 호환용)"""
    return get_path(subdir)


def is_connected() -> bool:
    """외부 저장소 연결 확인"""
    root = get_data_root()
    return os.path.isdir(root)


def ensure_dirs():
    """모든 하위 디렉토리 생성 (서버 시작 시 호출)"""
    root = get_data_root()
    for sub in SUBDIRS.values():
        os.makedirs(os.path.join(root, sub), exist_ok=True)


def get_status() -> dict:
    """데이터 경로 상태 정보"""
    root = get_data_root()
    os_name = _detect_os()

    # 저장되는 데이터 목록
    stored_data = {
        "상품 데이터":    f"data/{PROJECT_NAME}/{SUBDIRS['outputs']}/latest.json",
        "업로드 히스토리": f"data/{PROJECT_NAME}/{SUBDIRS['db']}/uploaded_history.json",
        "상품 이미지":    f"data/{PROJECT_NAME}/{SUBDIRS['outputs']}/images",
        "로그":          f"data/{PROJECT_NAME}/{SUBDIRS['logs']}",
        "DB 폴더":       f"data/{PROJECT_NAME}/{SUBDIRS['db']}",
        "출력 폴더":     f"data/{PROJECT_NAME}/{SUBDIRS['outputs']}",
    }

    return {
        "os": f"{'Mac (내부망)' if os_name == 'Darwin' else 'Windows (외부망)'}",
        "default_path": _default_path(),
        "current_path": root,
        "connected": is_connected(),
        "stored_data": stored_data,
    }


# 초기화 시 경로 로드
get_data_root()
