#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Этап 1. Скачивание исходных данных OpenStreetMap (Geofabrik, .osm.pbf).
Граф не строится, PBF не разбирается. Файлы сохраняются без изменений.

Принципы:
  * проверенный .osm.pbf никогда не перезаписывается и не удаляется;
  * непроверенный .osm.pbf сохраняется для ручной проверки;
  * .part сохраняется при любых сбоях; если он непригоден, он переименовывается
    в *.rejected-<причина>-<время>, но не удаляется;
  * состояние загрузки пишется в манифест ДО начала передачи данных.
"""

import hashlib
import json
import logging
import os
import re
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from tqdm import tqdm

# ============================ КОНФИГУРАЦИЯ ============================
# Чтобы добавить округ, допишите строку в REGIONS.
REGIONS = [
    {"id": "south-fed-district",          "name": "Южный федеральный округ"},
    {"id": "north-caucasus-fed-district", "name": "Северо-Кавказский федеральный округ"},
]

# URL сформирован по схеме Geofabrik и НЕ ПРОВЕРЕН автоматически.
# Если будет 404, скопируйте ссылку со страницы https://download.geofabrik.de/russia.html
BASE_URL = "https://download.geofabrik.de/russia/{region_id}-latest.osm.pbf"

CHUNK_SIZE = 1024 * 1024            # 1 МБ
MAX_ATTEMPTS = 3                    # попыток при временных ошибках
RETRY_DELAY_SEC = 5                 # базовая задержка (растёт с номером попытки)
TIMEOUT = (15, 60)                  # (соединение, чтение), секунды
DISK_RESERVE = 200 * 1024 * 1024    # запас свободного места, байт
# =====================================================================

ROOT_DIR = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT_DIR / "data" / "raw" / "osm"
MANIFEST_PATH = OUT_DIR / "download_manifest.json"

RESUMABLE_STATES = ("in_progress", "verified_pending_rename")
OK_STATES = ("ok_official_md5", "ok_local_sha256_only")

log = logging.getLogger("download")


class DownloadError(Exception):
    """Фатальная ошибка для региона: повторять бессмысленно."""


class ManualCheckRequired(DownloadError):
    """Итоговый .osm.pbf существует, но не подтверждён. Файл не тронут."""


class RetryableError(Exception):
    """Временная ошибка: можно повторить."""


# ------------------------------ утилиты ------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ts_compact() -> str:
    return datetime.now().strftime("%Y%m%dT%H%M%S")


def human(n) -> str:
    if n is None:
        return "-"
    size = float(n)
    for unit in ("Б", "КБ", "МБ", "ГБ"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} ТБ"


def rel(path: Path) -> str:
    try:
        return path.relative_to(ROOT_DIR).as_posix()
    except ValueError:
        return str(path)


def hash_file(path: Path) -> tuple[str, str]:
    """Потоковый подсчёт (sha256, md5) без загрузки файла в RAM."""
    sha, md5 = hashlib.sha256(), hashlib.md5()
    with path.open("rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            sha.update(chunk)
            md5.update(chunk)
    return sha.hexdigest(), md5.hexdigest()


def quarantine(path: Path, reason: str) -> Path:
    """Откладывает непригодный файл в сторону. НИЧЕГО НЕ УДАЛЯЕТ."""
    new = path.with_name(f"{path.name}.rejected-{reason}-{ts_compact()}")
    os.replace(path, new)
    log.warning("Файл сохранён для ручной проверки: %s", rel(new))
    return new


def ensure_disk_space(directory: Path, needed: int) -> None:
    free = shutil.disk_usage(directory).free
    if free < needed + DISK_RESERVE:
        raise DownloadError(
            f"недостаточно места на диске: нужно ~{human(needed + DISK_RESERVE)}, "
            f"свободно {human(free)}")


# ------------------------------ манифест ------------------------------
def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        try:
            data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("regions"), dict):
                return data
            raise ValueError("неожиданная структура")
        except (OSError, ValueError) as e:
            bad = MANIFEST_PATH.with_name(f"{MANIFEST_PATH.name}.corrupt-{ts_compact()}")
            try:
                os.replace(MANIFEST_PATH, bad)
                log.warning("Манифест повреждён (%s). Копия: %s. Создаётся новый.", e, rel(bad))
            except OSError:
                log.warning("Манифест повреждён (%s) и не может быть переименован.", e)
    return {"schema_version": 2, "regions": {}}


def save_manifest(manifest: dict) -> None:
    """Атомарная запись: временный файл, fsync, затем замена."""
    tmp = MANIFEST_PATH.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, MANIFEST_PATH)


def persist(manifest: dict, rid: str, entry: dict) -> None:
    manifest["regions"][rid] = entry
    save_manifest(manifest)


# ------------------------ версии источника ------------------------
def validators(headers) -> dict:
    return {"etag": headers.get("ETag"), "last_modified": headers.get("Last-Modified")}


def strong_validator(entry: dict):
    """Значение для If-Range: сильный ETag либо дата (слабые ETag в If-Range недопустимы)."""
    etag = entry.get("etag")
    if etag and not etag.startswith("W/"):
        return etag
    return entry.get("last_modified")


def same_version(a: dict, b: dict) -> bool:
    """True только если версия файла подтверждена. Нет валидаторов, значит False."""
    if a.get("etag") and b.get("etag"):
        return a["etag"] == b["etag"]
    if a.get("last_modified") and b.get("last_modified"):
        return a["last_modified"] == b["last_modified"]
    return False


def parse_content_range(value):
    m = re.fullmatch(r"\s*bytes\s+(\d+)-(\d+)/(\d+|\*)\s*", value or "")
    if not m:
        return None
    return int(m[1]), int(m[2]), (None if m[3] == "*" else int(m[3]))


# ------------------------- запросы к серверу -------------------------
def fetch_remote_info(session: requests.Session, url: str) -> dict:
    """HEAD: ожидаемый размер, ETag, Last-Modified."""
    try:
        r = session.head(url, allow_redirects=True, timeout=TIMEOUT,
                         headers={"Accept-Encoding": "identity"})
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
        raise RetryableError(f"нет связи с сервером: {e}") from e
    except requests.exceptions.RequestException as e:
        raise DownloadError(f"ошибка запроса: {e}") from e
    if r.status_code == 404:
        raise DownloadError(f"файл не найден (404): {url}. Сверьте ссылку со страницей Geofabrik.")
    if r.status_code >= 500 or r.status_code == 429:
        raise RetryableError(f"сервер вернул HTTP {r.status_code}")
    if r.status_code != 200:
        raise DownloadError(f"неожиданный ответ сервера: HTTP {r.status_code}")
    clen = r.headers.get("Content-Length")
    info = validators(r.headers)
    info["size"] = int(clen) if clen and clen.isdigit() else None
    return info


def fetch_remote_info_retry(session, url) -> dict:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return fetch_remote_info(session, url)
        except RetryableError as e:
            log.warning("Запрос информации, попытка %d/%d: %s", attempt, MAX_ATTEMPTS, e)
            if attempt == MAX_ATTEMPTS:
                raise DownloadError(f"сервер недоступен: {e}") from e
            time.sleep(RETRY_DELAY_SEC * attempt)


def fetch_official_md5(session: requests.Session, url: str):
    """Официальная MD5 Geofabrik (<url>.md5) или None."""
    try:
        r = session.get(url + ".md5", timeout=TIMEOUT)
        if r.status_code == 200:
            m = re.match(r"\s*([0-9a-fA-F]{32})\b", r.text)
            if m:
                return m.group(1).lower()
    except requests.exceptions.RequestException:
        pass
    return None


# ----------------------- проверка готового файла -----------------------
def check_final(entry: dict, path: Path) -> tuple[str, str]:
    """('ok', '') если файл подтверждён манифестом; иначе ('unverified', причина)."""
    if entry.get("status") not in OK_STATES + ("verified_pending_rename",):
        return "unverified", "в манифесте нет записи об успешной проверке этого файла"
    if not entry.get("sha256") or not entry.get("size_bytes"):
        return "unverified", "в манифесте нет размера или SHA-256"
    size = path.stat().st_size
    if size != entry["size_bytes"]:
        return "unverified", f"размер {size} Б не совпадает с записанным {entry['size_bytes']} Б"
    sha, _ = hash_file(path)
    if sha != entry["sha256"]:
        return "unverified", "SHA-256 не совпадает с записанным в манифесте"
    return "ok", ""


# ----------------------------- скачивание -----------------------------
def download_attempt(session, url: str, part: Path, entry: dict, save_state) -> None:
    existing = part.stat().st_size if part.exists() else 0
    expected = entry.get("expected_size")
    headers = {"Accept-Encoding": "identity"}
    resuming = existing > 0

    if resuming:
        validator = strong_validator(entry)
        if entry.get("status") != "in_progress" or not validator or entry.get("source_url") != url:
            quarantine(part, "no-state")           # нет оснований для докачки
            existing, resuming = 0, False
        else:
            headers["Range"] = f"bytes={existing}-"
            headers["If-Range"] = validator

    total = None
    try:
        with session.get(url, headers=headers, stream=True, timeout=TIMEOUT) as r:
            code = r.status_code

            if code == 416:
                m = re.fullmatch(r"\s*bytes\s+\*/(\d+)\s*", r.headers.get("Content-Range", ""))
                if resuming and m and expected and int(m[1]) == expected == existing:
                    return                          # .part уже полный, размер проверит вызывающий код
                quarantine(part, "range-416")
                raise RetryableError("сервер отклонил диапазон (416); загрузка начнётся заново")

            if code == 206:
                if not resuming:
                    raise DownloadError("сервер вернул 206 на запрос без Range")
                cr = parse_content_range(r.headers.get("Content-Range"))
                problems = []
                if cr is None:
                    problems.append("некорректный или отсутствующий Content-Range")
                else:
                    start, end, total = cr
                    if start != existing:
                        problems.append(f"начало диапазона {start} != размер .part {existing}")
                    if total is None:
                        problems.append("общий размер файла неизвестен")
                    else:
                        if expected and total != expected:
                            problems.append(f"общий размер {total} != ожидаемого {expected}")
                        if end != total - 1:
                            problems.append(f"конец диапазона {end} != {total - 1}")
                    clen = r.headers.get("Content-Length")
                    if clen and clen.isdigit() and int(clen) != end - start + 1:
                        problems.append("Content-Length не соответствует диапазону")
                if not same_version(entry, validators(r.headers)):
                    problems.append("ETag/Last-Modified ответа не совпадают с сохранёнными")
                if problems:
                    quarantine(part, "resume-mismatch")
                    raise RetryableError("докачка небезопасна: " + "; ".join(problems))
                mode = "ab"
                log.info("Докачка с позиции %s", human(existing))

            elif code == 200:
                if resuming:
                    # Сервер проигнорировал Range или файл изменился: дописывать нельзя.
                    log.warning("Сервер вернул полный файл вместо диапазона; "
                                "старая часть отложена, загрузка начинается заново.")
                    quarantine(part, "full-200-on-resume")
                    existing = 0
                clen = r.headers.get("Content-Length")
                total = int(clen) if clen and clen.isdigit() else None
                entry.update(validators(r.headers))
                entry.update(status="in_progress", source_url=url, expected_size=total,
                             part_file=part.name, started_at=now_iso())
                mode = "wb"

            elif code == 404:
                raise DownloadError(f"файл не найден (404): {url}")
            elif code >= 500 or code == 429:
                raise RetryableError(f"сервер вернул HTTP {code}")
            else:
                raise DownloadError(f"неожиданный ответ сервера: HTTP {code}")

            if total:
                ensure_disk_space(part.parent, total - existing)

            # Состояние фиксируется ДО передачи данных.
            entry["updated_at"] = now_iso()
            entry["note"] = None
            save_state()

            try:
                with part.open(mode) as f, tqdm(
                        total=total, initial=existing, unit="B", unit_scale=True,
                        unit_divisor=1024, desc="  Загрузка", ascii=True) as bar:
                    for chunk in r.iter_content(CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            bar.update(len(chunk))
                    f.flush()
                    os.fsync(f.fileno())
            except requests.exceptions.RequestException:
                raise                                # сетевые ошибки, не дисковые
            except PermissionError as e:
                raise DownloadError(f"нет прав на запись: {e}") from e
            except OSError as e:
                if getattr(e, "errno", None) == 28:
                    raise DownloadError("недостаточно места на диске") from e
                raise DownloadError(f"ошибка записи на диск: {e}") from e

    except (requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ContentDecodingError) as e:
        raise RetryableError(f"сбой сети: {e}") from e
    except requests.exceptions.RequestException as e:
        raise DownloadError(f"ошибка запроса: {e}") from e

    got = part.stat().st_size
    if total and got != total:
        raise RetryableError(f"загрузка оборвана: получено {got} из {total} байт")


def download_with_retries(session, url, part, entry, save_state) -> None:
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            download_attempt(session, url, part, entry, save_state)
            return
        except RetryableError as e:
            log.warning("Попытка %d/%d не удалась: %s", attempt, MAX_ATTEMPTS, e)
            entry["note"] = f"попытка {attempt}/{MAX_ATTEMPTS}: {e}"
            entry["updated_at"] = now_iso()
            save_state()                             # .part и состояние сохранены
            if attempt == MAX_ATTEMPTS:
                raise DownloadError(f"не удалось скачать за {MAX_ATTEMPTS} попытки: {e}") from e
            time.sleep(RETRY_DELAY_SEC * attempt)


# --------------------------- обработка региона ---------------------------
def process_region(session, region: dict, manifest: dict) -> dict:
    rid, name = region["id"], region["name"]
    url = BASE_URL.format(region_id=rid)
    file_name = f"{rid}.osm.pbf"
    final_path = OUT_DIR / file_name
    part_path = OUT_DIR / f"{file_name}.part"

    print(f"\nРегион: {name}")
    entry = manifest["regions"].get(rid) or {}

    def save_state():
        persist(manifest, rid, entry)

    # --- 1. Уже есть итоговый файл ---
    print("Проверка существующего файла...")
    if final_path.exists():
        state, msg = check_final(entry, final_path)
        if state == "ok":
            if entry.get("status") == "verified_pending_rename":
                entry["status"] = entry.pop("final_status", "ok_local_sha256_only")
                entry["downloaded_at"] = entry.get("downloaded_at") or now_iso()
                save_state()
                print("Состояние восстановлено после прерывания: файл проверен ранее.")
            else:
                print("Файл уже скачан и проверен, повторная загрузка не нужна.")
            return entry
        print(f"ВНИМАНИЕ: {rel(final_path)} уже существует, но не подтверждён: {msg}.")
        print("Файл оставлен без изменений для ручной проверки и не перезаписывается.")
        print("Чтобы скачать заново, переместите или переименуйте его вручную.")
        raise ManualCheckRequired(msg)

    # --- 2. Информация об источнике ---
    remote = fetch_remote_info_retry(session, url)
    expected = remote["size"]

    # --- 3. Можно ли продолжить старую загрузку ---
    keep_part = False
    if part_path.exists():
        psize = part_path.stat().st_size
        reason = text = None
        if psize == 0:
            part_path.unlink()                       # пустой файл: терять нечего
        elif entry.get("status") not in RESUMABLE_STATES or entry.get("source_url") != url:
            reason, text = "orphan", "в манифесте нет записи о незавершённой загрузке"
        elif not same_version(entry, remote):
            reason, text = "source-changed", "файл на сервере изменился с момента начала загрузки"
        elif entry.get("expected_size") and expected and entry["expected_size"] != expected:
            reason, text = "size-changed", "изменился ожидаемый размер файла"
        elif expected is not None and psize > expected:
            reason, text = "oversize", "частичный файл больше ожидаемого размера"
        if reason:
            print(f"Незавершённая загрузка не может быть продолжена: {text}.")
            quarantine(part_path, reason)
        elif psize > 0:
            keep_part = True
            entry["status"] = "in_progress"
            for k in ("final_status", "size_bytes", "sha256", "md5", "official_md5"):
                entry.pop(k, None)

    if not keep_part:
        entry.clear()
        entry.update({
            "name": name, "geofabrik_id": rid, "source_url": url, "file_name": file_name,
            "status": "in_progress",
            "etag": remote["etag"], "last_modified": remote["last_modified"],
            "expected_size": expected, "part_file": part_path.name,
            "started_at": now_iso(), "updated_at": now_iso(), "note": None,
        })
    save_state()                                     # состояние записано до передачи данных

    # --- 4. Скачивание и проверка ---
    try:
        psize = part_path.stat().st_size if part_path.exists() else 0
        if keep_part and expected is not None and psize == expected:
            print("Файл докачан полностью ранее, переходим к проверке...")
        else:
            if keep_part:
                print(f"Продолжение скачивания с {human(psize)} (версия файла подтверждена)...")
            else:
                print("Начало скачивания...")
            download_with_retries(session, url, part_path, entry, save_state)

        print("Скачивание завершено.\nПроверка размера...")
        size = part_path.stat().st_size
        exp = entry.get("expected_size")
        if exp is not None and size != exp:
            kept = quarantine(part_path, "size-mismatch")
            entry.update(status="failed_size", updated_at=now_iso(),
                         note=f"размер {size} != ожидаемого {exp}; файл: {rel(kept)}")
            save_state()
            raise DownloadError(entry["note"])

        print("Вычисление SHA-256...")
        sha256, md5 = hash_file(part_path)

        official_md5, md5_note = None, None
        official = fetch_official_md5(session, url)
        if official:
            try:
                after = fetch_remote_info(session, url)
            except (RetryableError, DownloadError):
                after = None
            if after and same_version(entry, after):
                official_md5 = official
            else:
                md5_note = ("официальный MD5 не применён: версия файла на сервере "
                            "не подтверждена или уже обновилась")
        if official_md5 and md5 != official_md5:
            kept = quarantine(part_path, "md5-mismatch")
            entry.update(status="failed_md5", updated_at=now_iso(),
                         note=f"MD5 не совпал с официальным; файл: {rel(kept)}")
            save_state()
            raise DownloadError(entry["note"])

        final_status = "ok_official_md5" if official_md5 else "ok_local_sha256_only"

        # Фиксируем результат ДО переименования (защита от Ctrl+C в этот момент).
        entry.update(status="verified_pending_rename", final_status=final_status,
                     size_bytes=size, sha256=sha256, md5=md5, official_md5=official_md5,
                     local_path=rel(final_path), updated_at=now_iso(), note=md5_note)
        save_state()

        if final_path.exists():
            raise DownloadError(f"{rel(final_path)} уже существует: перезапись запрещена")
        os.replace(part_path, final_path)

        entry.pop("final_status", None)
        entry.pop("part_file", None)
        entry.update(status=final_status, downloaded_at=now_iso(),
                     source_last_modified=entry.get("last_modified"), updated_at=now_iso())
        save_state()
        print("Файл сохранён.")
        if final_status == "ok_local_sha256_only":
            print("Официальная контрольная сумма не использована: сохранён только локальный SHA-256.")
        return entry

    except KeyboardInterrupt:
        entry["note"] = "прервано пользователем (Ctrl+C); .part сохранён для докачки"
        entry["updated_at"] = now_iso()
        save_state()
        raise
    except DownloadError as e:
        if entry.get("status") == "in_progress":
            entry["note"] = str(e)
            entry["updated_at"] = now_iso()
            save_state()
        raise


# ------------------------------- main -------------------------------
def print_summary(rows: list[dict]) -> None:
    print("\n" + "=" * 110)
    print("ИТОГИ")
    print("=" * 110)
    print(f"{'Округ':<38}{'Размер':>12}  {'Статус проверки':<26}Путь")
    for r in rows:
        print(f"{r['name']:<38}{human(r.get('size_bytes')):>12}  {r['status']:<26}{r['path']}")
    print("=" * 110)


def main() -> int:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    print("Скачивание исходных данных OpenStreetMap")
    try:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        print(f"Не удалось создать каталог {OUT_DIR}: {e}")
        return 1

    manifest = load_manifest()
    session = requests.Session()
    session.headers["User-Agent"] = "osm-transport-graph-student-project/1.1"
    rows, failed = [], False

    for region in REGIONS:
        final_path = OUT_DIR / f"{region['id']}.osm.pbf"
        try:
            e = process_region(session, region, manifest)
            rows.append({"name": region["name"], "size_bytes": e.get("size_bytes"),
                         "status": e.get("status", "?"), "path": e.get("local_path", "")})
        except ManualCheckRequired:
            failed = True
            rows.append({"name": region["name"],
                         "size_bytes": final_path.stat().st_size if final_path.exists() else None,
                         "status": "ТРЕБУЕТ РУЧНОЙ ПРОВЕРКИ", "path": rel(final_path)})
        except DownloadError as e:
            failed = True
            print(f"ОШИБКА [{region['name']}]: {e}")
            rows.append({"name": region["name"], "size_bytes": None,
                         "status": "ОШИБКА", "path": "-"})
        except KeyboardInterrupt:
            print("\nПрервано пользователем. Состояние сохранено в манифесте, "
                  ".part оставлен для докачки при следующем запуске.")
            return 130
        except Exception as e:  # непредвиденная ошибка не должна ломать остальные округа
            failed = True
            log.exception("Непредвиденная ошибка при обработке %s", region["name"])
            print(f"ОШИБКА [{region['name']}]: {e}")
            rows.append({"name": region["name"], "size_bytes": None,
                         "status": "ОШИБКА", "path": "-"})

    print_summary(rows)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())