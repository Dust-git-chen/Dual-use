#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dual-use · Windows 桌面端双模网络测速与分流延迟诊断终端
后端核心引擎 (main.py)  —— v3.0 千兆修复版

v3.0 相对 v2.x 的关键修复：
1. 【测速源失效】原 4 个直连源经实测 3 个已 404/403，仅剩 1 个可用，
   导致实际并发从 32 线程塌缩到 8 线程，测出带宽被源站限速封顶在 100~280Mbps。
   现改为「启动时并发 HEAD 预检 + 自动剔除失效源 + 多源轮换并发」。
2. 【分母错误】原实现用「任务启动墙钟时间」做分母，worker 因 404 快速抛异常后
   空转的时间也被计入，导致字节不增而时间流逝，速度被系统性压爆。
   现改为「有效传输窗口」计时：丢弃前 2 秒 TCP 慢启动，只统计稳态区间。
3. 【测试时长过短】原 duration_limit=3.5s，慢启动还没结束就收尾。
   现默认 12 秒，前 2 秒预热不计入，取后段稳态均值。
4. 【直连并未真正直连】机器上存在 sing-box TUN 时，普通 socket 会被路由进
   TUN 用户态转发。现支持按物理网卡出口 IP 绑定 socket（bind），实现真正直连。
5. 【前后端割裂】原 exe 内嵌前端对 pywebview.api 的调用次数为 0，全部走
   外部 C# 桥。现后端提供完整原生 API，前端优先调用本模块。
"""

import os
import sys
import time
import json
import socket
import threading
import http.client
import ssl
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, urlencode

try:
    import webview
except ImportError:
    webview = None


# ---------------------------------------------------------------------------
# 测速源池（2026-10-22 复测，出口 27.18.101.255 武汉电信 / 1000BASE-T）
#
# 复测结论（v3.1）：
#   · 迅雷 CDN（down.sandai.net）单源 8 线程 795 / 32 线程 854 Mbps，
#     是本机千兆链路上唯一能稳定顶到线路上限（940）的源 —— 作为主力。
#   · 腾讯 CDN 单源 32 线程 745 但会突发限流（复测时整源 0 吞吐），
#     作为备源保留，靠预检阶段实测吞吐排序、慢源自动剔除。
#   · 高校镜像站（清华/中科大/北外/华为云）对程序化下载仍全面 403/418，
#     阿里云/南大/北大等 302 后的真实文件也已 404，不进池。
#   · 360 软件源虽可下载但单源仅 6.6 Mbps，属慢源，不进池。
# 网卡 1000BASE-T → 应用层可达上限约 940 Mbps，1000 Mbps 是物理不可能值。
DIRECT_DOWNLOAD_SOURCES = [
    # 迅雷 CDN：单源即可打满千兆，多线程叠加收益有限但仍按并发摊薄长尾。
    "https://down.sandai.net/thunder11/XunLeiSetup12.0.10.2266.exe",
    # 腾讯 CDN：命中良好时单源 745 Mbps，但存在按 IP 突发限流，作备源。
    "https://dldir1v6.qq.com/weixin/Windows/WeChatSetup.exe",
]

# 受封锁、暂不可用的源（记录实测状态，不参与测速）
BLOCKED_SOURCES = {
    "https://repo.huaweicloud.com/ubuntu-releases/22.04/ubuntu-22.04.5-desktop-amd64.iso": "403 拦截页",
    "https://mirrors.ustc.edu.cn/ubuntu-releases/22.04/ubuntu-22.04.5-desktop-amd64.iso": "200 但 body 为 0",
    "https://mirrors.bfsu.edu.cn/ubuntu-releases/22.04/ubuntu-22.04.5-desktop-amd64.iso": "403 拦截页",
    "https://mirrors.tuna.tsinghua.edu.cn/ubuntu-releases/22.04/ubuntu-22.04.5-desktop-amd64.iso": "403 非常用软件特征",
    "https://mirrors.huaweicloud.com/ubuntu-releases/22.04/ubuntu-22.04.5-desktop-amd64.iso": "418 限速",
}

PROXY_DOWNLOAD_SOURCES = [
    # 走海外代理实测（出口 35.243.109.96 日本 NRT，32 线程）：
    #   unpkg        206 Mbps  ← 最优，放首位
    #   jsdelivr     142 Mbps
    #   cloudflare    0 Mbps（__down 对非浏览器 UA/该节点直接 403，作备源）
    "https://unpkg.com/@ffmpeg/core@0.12.6/dist/umd/ffmpeg-core.wasm",
    "https://cdn.jsdelivr.net/npm/@ffmpeg/core@0.12.6/dist/umd/ffmpeg-core.wasm",
    "https://speed.cloudflare.com/__down?bytes=100000000",
]

UPLOAD_SOURCES = [
    "https://speed.cloudflare.com/__up",
]

# 部分测速服务（如 Cloudflare speed）按 User-Agent 返回 403，
# 统一使用浏览器 UA，避免被误判为脚本流量而测不出数据。
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

WARMUP_SECONDS = 2.0       # TCP 慢启动预热，不计入稳态
DEFAULT_DURATION = 12      # 单次测速总时长（秒）
LIVE_WINDOW_SECONDS = 1.0  # 实时速率滑动窗口长度
LIVE_EMA_ALPHA = 0.35      # 实时速率 EMA 平滑系数（越大越灵敏）
SYNC_PHASE_SECONDS = 1.5   # 预热后「统一开闸」的同步相位，消除各 worker 慢启动
DOWNLOAD_MIN_SOURCE_MBPS = 8.0    # 单源有效吞吐低于此值视为慢源，不再接新请求
READ_CHUNK = 512 * 1024    # 单次 recv 字节数（越大 syscall 越少）
SOCK_RCVBUF = 4 * 1024 * 1024     # socket 接收缓冲，消除 Windows 默认 64KB 抖动
COUNT_FLUSH_BYTES = 2 * 1024 * 1024   # worker 本地累计到 2MB 才碰一次全局锁
COUNT_FLUSH_SECONDS = 0.12
DEFAULT_THREADS = 48       # 千兆链路在 32~48 并发区间达到平台期
MAX_THREADS = 128
# 预检需要真正跑一段流量才能分辨「能连」和「能跑满」：
# 存活但吞吐过低的源会白占并发名额（实测混入慢源后 854 → 787 Mbps），
# 必须剔除。代理线路本身通常只有百兆级，阈值单独放宽。
PROBE_MIN_MBPS = 40.0
PROXY_PROBE_MIN_MBPS = 12.0
# 短测必须跨过 TLS 握手与慢启动：实测 2.5 秒会把 268 Mbps 的源测成 12 Mbps，
# 导致选源选错。4 秒是「够准」和「启动别太慢」的折中。
PROBE_MEASURE_SECONDS = 4.0
PROBE_MEASURE_THREADS = 8
# 主源独占阈值：最优源的 x 倍以内才允许一起跑，否则只用最优源。
SOURCE_KEEP_RATIO = 0.75


def get_windows_system_proxy() -> str:
    """从 Windows 注册表读取当前系统代理地址（若开启）"""
    if sys.platform != "win32":
        return ""
    try:
        import winreg
        key_path = r"Software\Microsoft\Windows\CurrentVersion\Internet Settings"
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path, 0, winreg.KEY_READ) as key:
            proxy_enable, _ = winreg.QueryValueEx(key, "ProxyEnable")
            if proxy_enable:
                proxy_server, _ = winreg.QueryValueEx(key, "ProxyServer")
                if proxy_server:
                    first = str(proxy_server).split(";")[0].strip()
                    # ProxyServer 可能是 "host:port" 或 "http=host:port;https=..."
                    if "=" in first:
                        first = first.split("=", 1)[1]
                    return first
    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
# 代理隧道层（v3.1 新增）
#
# 旧版致命缺陷：test_bandwidth / ping_target 虽然收下了 proxy_endpoint 参数，
# 但函数体里从未使用 —— 「走代理」模式实际仍在 socket 直连。当机器上存在
# sing-box TUN 时，这些直连流量会被 TUN 的规则判定为国内目标而放行成直连，
# 出口 IP 仍是本地电信（实测 27.18.101.255 武汉），海外源自然测不出速度。
# 本节补上真正的 SOCKS5 / HTTP CONNECT 隧道，并且**禁止静默回退直连**：
# 代理不可用时必须如实报错，而不是偷偷直连给出一个假的海外成绩。
# ---------------------------------------------------------------------------

_PROXY_SCHEME_CACHE = {}          # endpoint -> 已验证可用的协议（socks5/http）


def parse_proxy_endpoint(endpoint):
    """解析代理串 -> (scheme, host, port, auth)

    支持：127.0.0.1:10808 / socks5://user:pass@host:port /
          socks=1.2.3.4:1080 / http=host:port / [::1]:1080
    scheme 为空表示需要自动嗅探协议。
    """
    ep = (endpoint or "").strip()
    if not ep:
        return ("", "", 0, None)
    scheme = ""
    if "://" in ep:
        scheme, ep = ep.split("://", 1)
    elif ep.lower().startswith("socks") and "=" in ep:
        scheme, _, ep = ep.partition("=")
    scheme = scheme.lower().strip()
    if scheme in ("socks", "socks5h", "socks4"):
        scheme = "socks5"
    elif scheme in ("http", "https"):
        scheme = "http"

    auth = None
    if "@" in ep:
        cred, _, ep = ep.rpartition("@")
        if ":" in cred:
            auth = (cred.split(":", 1)[0], cred.split(":", 1)[1])
    ep = ep.split("/")[0]

    if ep.startswith("["):                      # IPv6 字面量 [::1]:1080
        host, _, port = ep.rpartition("]:")
        host = host.lstrip("[")
    else:
        host, sep, port = ep.rpartition(":")
        if not sep:
            host, port = ep, "80"
    try:
        port_i = int(port)
    except ValueError:
        port_i = 0
    return (scheme, host, port_i, auth)


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        c = sock.recv(n - len(buf))
        if not c:
            raise OSError("代理连接提前关闭")
        buf += c
    return buf


def socks5_connect(px_host, px_port, host, port, timeout=8.0, auth=None):
    """经 SOCKS5 建到 host:port 的隧道；域名交给代理解析（避免本地 DNS 污染）"""
    s = socket.create_connection((px_host, px_port), timeout=timeout)
    try:
        s.settimeout(timeout)
        s.sendall(b"\x05\x01\x00" if not auth else b"\x05\x02\x00\x02")
        ver, method = _recv_exact(s, 2)
        if ver != 5:
            raise OSError("目标端口不是 SOCKS5（版本字节 %d）" % ver)
        if method == 0x02:
            if not auth:
                raise OSError("SOCKS5 要求认证但未提供账号")
            u, p = auth[0].encode(), auth[1].encode()
            s.sendall(b"\x01" + bytes([len(u)]) + u + bytes([len(p)]) + p)
            if _recv_exact(s, 2)[1] != 0:
                raise OSError("SOCKS5 认证失败")
        elif method != 0x00:
            raise OSError("SOCKS5 无可接受认证方式")

        hb = host.encode("idna")
        s.sendall(b"\x05\x01\x00\x03" + bytes([len(hb)]) + hb + port.to_bytes(2, "big"))
        rep = _recv_exact(s, 4)
        if rep[1] != 0:
            raise OSError("SOCKS5 CONNECT 被拒绝（code=%d）" % rep[1])
        atyp = rep[3]
        if atyp == 1:
            skip = 4
        elif atyp == 4:
            skip = 16
        else:
            skip = _recv_exact(s, 1)[0]
        _recv_exact(s, skip + 2)
        s.settimeout(None)
        return s
    except Exception:
        try:
            s.close()
        except Exception:
            pass
        raise


def http_connect(px_host, px_port, host, port, timeout=8.0, auth=None):
    """经 HTTP CONNECT 建隧道（Clash/v2rayN 的 http 入站也通用）"""
    import base64
    s = socket.create_connection((px_host, px_port), timeout=timeout)
    try:
        s.settimeout(timeout)
        head = "CONNECT %s:%d HTTP/1.1\r\nHost: %s:%d\r\n" % (host, port, host, port)
        if auth:
            token = base64.b64encode(("%s:%s" % (auth[0], auth[1])).encode()).decode()
            head += "Proxy-Authorization: Basic %s\r\n" % token
        s.sendall((head + "\r\n").encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            c = s.recv(4096)
            if not c:
                raise OSError("代理在 CONNECT 阶段断开")
            buf += c
            if len(buf) > 16384:
                raise OSError("CONNECT 响应头超长")
        line = buf.split(b"\r\n", 1)[0].decode("latin1", "replace")
        parts = line.split()
        if len(parts) < 2 or parts[1] != "200":
            raise OSError("HTTP CONNECT 失败：%s" % line)
        s.settimeout(None)
        return s
    except Exception:
        try:
            s.close()
        except Exception:
            pass
        raise


def open_tunnel(host, port, mode="direct", proxy_endpoint="", bind_ip="",
                timeout=8.0):
    """按模式建立到 host:port 的 TCP 连接（direct 绑定物理网卡，proxy 走隧道）"""
    if mode != "proxy":
        return socket.create_connection(
            (host, port), timeout=timeout,
            source_address=(bind_ip, 0) if bind_ip else None)

    scheme, px_host, px_port, auth = parse_proxy_endpoint(proxy_endpoint)
    if not px_host or not px_port:
        raise OSError("代理模式缺少代理地址（系统代理与入参均为空）")

    if scheme in ("socks5", "http"):
        order = [scheme]
    else:
        # 协议未知：先按已缓存的成功协议试，再穷举
        cached = _PROXY_SCHEME_CACHE.get(proxy_endpoint)
        order = [cached] if cached else []
        order += [k for k in ("socks5", "http") if k not in order]

    last = None
    for kind in order:
        try:
            if kind == "socks5":
                sock = socks5_connect(px_host, px_port, host, port, timeout, auth)
            else:
                sock = http_connect(px_host, px_port, host, port, timeout, auth)
            _PROXY_SCHEME_CACHE[proxy_endpoint] = kind
            return sock
        except Exception as e:
            last = e
    raise OSError("代理 %s:%d 不可用：%s" % (px_host, px_port, last))


def detect_tun_interfaces() -> list:
    """
    检测是否存在 TUN/TAP 虚拟网卡（存在时直连流量可能被劫持进用户态转发）。
    直接读注册表，避免 PowerShell 引号/编码/本地化输出问题。
    """
    names = []
    if sys.platform != "win32":
        return names
    base = r"SYSTEM\CurrentControlSet\Control\Network\{4D36E972-E325-11CE-BFC1-08002BE10318}"
    try:
        import winreg
        root = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, base)
        i = 0
        while True:
            try:
                guid = winreg.EnumKey(root, i)
            except OSError:
                break
            i += 1
            try:
                with winreg.OpenKey(root, guid + r"\Connection") as ck:
                    name, _ = winreg.QueryValueEx(ck, "Name")
                    pid, _ = winreg.QueryValueEx(ck, "PnpInstanceID")
                    low_pid = str(pid).lower()
                    low_name = str(name).lower()
                    # Wintun/WireGuard 驱动，或名称含 tun/tap 的虚拟网卡
                    if ("wintun" in low_pid or "wireguard" in low_pid
                            or "tap" in low_pid
                            or any(k in low_name for k in ("tun", "tap", "wireguard", "sing-box"))):
                        names.append(str(name))
            except OSError:
                continue
        if root:
            winreg.CloseKey(root)
    except Exception:
        pass
    return names


def detect_physical_ipv4() -> str:
    """探测用于旁路 TUN 的物理网卡 IPv4（默认路由指向真实网关的那个）"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # 连接外部地址不会被真正发送，只用于让内核选路
            s.connect(("223.5.5.5", 80))
            ip = s.getsockname()[0]
        finally:
            s.close()
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass
    return ""


class AeroPulseDesktopAPI:
    """暴露给前端 PyWebView 调用的原生双通道测速接口"""

    def __init__(self):
        self.default_proxy = get_windows_system_proxy()
        self._source_cache = {}
        self._proxy_report = None      # 最近一次代理探活结果（get_network_info 复用）
        self._cancel = threading.Event()
        # 实时吞吐采样：前端轮询 get_progress() 拿到「已实测」的瞬时速率，
        # 用于驱动仪表盘指针/刻度/波形。这里的每个数都是 worker 真实收到的字节
        # 换算而来，不含任何插值或伪造值。
        self._live = {
            "prev_bytes": 0, "prev_t": 0.0,
            "mbps": 0.0,          # EMA 平滑后的展示值（驱动游标）
            "raw_mbps": 0.0,      # 滑动窗口原始值，便于诊断
            "peak": 0.0, "bytes": 0, "elapsed": 0.0,
            "phase": "idle", "ratio": 0.0,
            "window": [],         # [(t, bytes)] 1 秒滑动窗口
            "samples": [],        # 每秒一个稳态采样点，供波形回放
        }
        self._live_lock = threading.Lock()
        self._t0 = 0.0

    # -- 测速源预检 ---------------------------------------------------------
    @staticmethod
    def _norm_mode(mode) -> str:
        """
        统一模式口径。前端传的是 'vpn'，旧代码只认 'proxy' 字面量，
        未匹配时 open_tunnel 一律按直连处理 —— 这是「走 VPN 测海外延迟
        全部超时/结果不对」的根因：实际走的是物理网卡直连。
        'vpn' / 'proxy' / '代理' 等一律归一化为 'proxy'，其余按 'direct'。
        """
        m = str(mode or "").strip().lower()
        return "proxy" if m in ("vpn", "proxy", "tunnel", "代理", "隧道") else "direct"

    def _probe_source(self, url: str, timeout: float = 8.0, _depth: int = 0,
                      mode: str = "direct", proxy: str = "", bind_ip: str = "") -> dict:
        """
        真实 GET 预检单个测速源（只取前 256KB 即断开），直连/代理同一套逻辑。
        不使用 HEAD：部分镜像站对 HEAD 返回 418/405，会把可用源误判为失效。
        最多跟随 4 次重定向。
        """
        sock = None
        try:
            u = urlparse(url)
            host = u.hostname
            port = u.port or (443 if u.scheme == "https" else 80)
            path = (u.path or "/") + ("?" + u.query if u.query else "")
            t0 = time.perf_counter()
            sock = open_tunnel(host, port, mode, proxy, bind_ip, timeout=timeout)
            sock.settimeout(timeout)
            if u.scheme == "https":
                sock = ssl._create_unverified_context().wrap_socket(sock, server_hostname=host)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCK_RCVBUF)
            sock.sendall((
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {host}\r\n"
                f"User-Agent: {BROWSER_UA}\r\n"
                f"Accept: */*\r\n"
                f"Range: bytes=0-262143\r\n"
                f"Connection: close\r\n\r\n"
            ).encode())

            buf = b""
            while b"\r\n\r\n" not in buf:
                c = sock.recv(16384)
                if not c:
                    break
                buf += c
                if len(buf) > 65536:
                    break
            head, _, body = buf.partition(b"\r\n\r\n")
            head_txt = head.decode("latin1", "replace")
            status = 0
            parts = head_txt.split("\r\n", 1)[0].split()
            if len(parts) > 1 and parts[1].isdigit():
                status = int(parts[1])
            loc, length = "", ""
            for line in head_txt.split("\r\n")[1:]:
                low = line.lower()
                if low.startswith("location:"):
                    loc = line.split(":", 1)[1].strip()
                elif low.startswith("content-length:"):
                    length = line.split(":", 1)[1].strip()

            got = len(body)
            while got < 262144:
                c = sock.recv(65536)
                if not c:
                    break
                got += len(c)
            ms = int((time.perf_counter() - t0) * 1000)

            # 3xx 必须跟随：CDN 常把 dldir1 跳到真实边缘节点
            if status in (301, 302, 303, 307, 308) and loc and _depth < 4:
                if loc.startswith("/"):
                    loc = f"{u.scheme}://{host}{loc}"
                return self._probe_source(loc, timeout, _depth + 1, mode, proxy, bind_ip)

            # 403/418 不能直接判死：部分镜像站在 Range 请求时会返回 403/418，
            # 但整段下载完全正常。拿到正数字节且不是硬失败，就认为可用；
            # 最终能否测速由后续的吞吐短测决定。
            body_ok = got > 0 and status not in (400, 404, 410, 500, 502, 503, 504)
            ok = (200 <= status < 300 and got > 0) or body_ok
            result = {"url": url, "ok": ok, "status": status, "ms": ms,
                      "probe_bytes": got,
                      "size": int(length) if length and length.isdigit() else 0}
            if ok and _depth > 0:
                result["resolved"] = url
            return result
        except Exception as e:
            return {"url": url, "ok": False, "status": 0, "ms": -1,
                    "probe_bytes": 0, "size": 0, "error": str(e)[:100]}
        finally:
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass

    def _resolve_proxy(self, proxy_endpoint: str = "") -> str:
        """参数优先，其次系统代理。返回空串表示没有可用代理配置。"""
        return (proxy_endpoint or "").strip() or self.default_proxy

    def _trace_ip(self, mode: str, proxy: str, bind_ip: str = "", timeout: float = 8.0) -> dict:
        """经指定通道访问 cloudflare trace，取出口 IP / 地区 / 边缘机房"""
        host = "www.cloudflare.com"
        sock = None
        try:
            sock = open_tunnel(host, 443, mode, proxy, bind_ip, timeout=timeout)
            sock.settimeout(timeout)
            sock = ssl._create_unverified_context().wrap_socket(sock, server_hostname=host)
            sock.sendall((f"GET /cdn-cgi/trace HTTP/1.1\r\nHost: {host}\r\n"
                          f"User-Agent: {BROWSER_UA}\r\nAccept: */*\r\n"
                          f"Connection: close\r\n\r\n").encode())
            buf = b""
            while len(buf) < 8192:
                c = sock.recv(4096)
                if not c:
                    break
                buf += c
                if buf.count(b"\r\n") > 40:
                    break
            body = buf.partition(b"\r\n\r\n")[2].decode("latin1", "replace")
            out = {}
            for line in body.splitlines():
                if "=" in line:
                    k, v = line.split("=", 1)
                    if k in ("ip", "loc", "colo", "warp"):
                        out[k] = v.strip()
            return out if out.get("ip") else {"error": "trace 无有效返回"}
        except Exception as e:
            return {"error": str(e)[:100]}
        finally:
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass

    def check_proxy(self, proxy_endpoint: str = "") -> dict:
        """
        代理链路探活（这是判断「国外测速到底有没有走代理」的唯一可信手段）：
        分别经代理与直连取 cloudflare trace 的出口 IP，IP 不同才算真的走了代理。
        TUN 模式下直连流量会被规则放行，光看「能连通」完全无法说明走了代理。
        """
        ep = self._resolve_proxy(proxy_endpoint)
        direct = self._trace_ip("direct", "", detect_physical_ipv4())
        if not ep:
            rep = {"ok": False, "endpoint": "", "via_proxy": False,
                   "direct_ip": direct.get("ip", ""), "direct_loc": direct.get("loc", ""),
                   "error": "未检测到系统代理，且未传入代理地址"}
            self._proxy_report = rep
            return rep

        t0 = time.perf_counter()
        px = self._trace_ip("proxy", ep)
        ms = int((time.perf_counter() - t0) * 1000)
        scheme, px_host, px_port, _ = parse_proxy_endpoint(ep)
        used = _PROXY_SCHEME_CACHE.get(ep, scheme or "")

        ok = "ip" in px
        same = ok and px.get("ip") == direct.get("ip")
        rep = {
            "ok": ok,
            "endpoint": ep,
            "scheme": used,
            "latency_ms": ms if ok else -1,
            "exit_ip": px.get("ip", ""),
            "exit_loc": px.get("loc", ""),
            "exit_colo": px.get("colo", ""),
            "direct_ip": direct.get("ip", ""),
            "direct_loc": direct.get("loc", ""),
            "via_proxy": bool(ok and not same),
            "error": "" if ok else px.get("error", "代理不可用"),
        }
        if ok and same:
            rep["error"] = "代理出口与直连出口相同，流量没有真正经过代理线路"
        self._proxy_report = rep
        return rep

    def _quick_rate(self, url: str, mode: str, proxy: str, bind_ip: str,
                    threads: int = 4, secs: float = PROBE_MEASURE_SECONDS) -> float:
        """短测单源真实吞吐，用于在预检阶段淘汰「连得上但跑不满」的源"""
        counter = {"bytes": 0}
        lock = threading.Lock()
        start = time.perf_counter()
        gate = start + 0.4
        deadline = start + secs

        def worker(i):
            self._download_worker([url], i, bind_ip, deadline, counter, lock,
                                  gate, None, mode=self._norm_mode(mode), proxy=proxy)

        self._cancel.clear()
        with ThreadPoolExecutor(max_workers=threads) as ex:
            list(ex.map(worker, range(threads)))
        el = max(0.5, secs - 0.4)
        return (counter["bytes"] * 8) / (el * 1_000_000)

    def probe_sources(self, mode: str = "direct", proxy_endpoint: str = "") -> dict:
        """
        并发预检全部测速源，返回存活列表。
        v3.1 增加「存活后短测吞吐」：能连通但吞吐 < PROBE_MIN_MBPS 的源
        （例如被按 IP 限流的 CDN）会稀释并发名额，必须剔除而非纳入。
        proxy 模式下若代理不可用，直接返回错误，绝不静默改走直连。
        """
        pool = DIRECT_DOWNLOAD_SOURCES if self._norm_mode(mode) == "direct" else PROXY_DOWNLOAD_SOURCES
        is_proxy = self._norm_mode(mode) == "proxy"
        proxy = self._resolve_proxy(proxy_endpoint) if is_proxy else ""
        bind_ip = detect_physical_ipv4() if not is_proxy else ""

        if is_proxy:
            chk = self._proxy_report or self.check_proxy(proxy)
            if not chk.get("ok") or not chk.get("via_proxy", False):
                self._source_cache[mode] = []
                return {"mode": mode, "alive": [], "dead": [], "alive_count": 0,
                        "total": len(pool), "proxy": chk,
                        "error": chk.get("error") or "代理链路不可用"}

        with ThreadPoolExecutor(max_workers=len(pool) or 1) as ex:
            results = list(ex.map(
            lambda u: self._probe_source(u, mode=self._norm_mode(mode), proxy=proxy, bind_ip=bind_ip),
                pool))

        # 对存活源做 8 线程短测，按实测吞吐排序。阈值是「相对最优 + 模式底限」，
        # 而不是单一绝对阈值 —— 否则代理线路（百兆级）会被全判死，
        # 出现「明明能测却报无可用源」的假失败。
        floor = PROBE_MIN_MBPS if self._norm_mode(mode) == "direct" else PROXY_PROBE_MIN_MBPS
        scored = [r for r in results if r["ok"]]

        # 并发短测：串行跑 3 个源要 17 秒，启动体感太差。并发会让每个源的
        # 绝对值略低（互相抢带宽），但这里只用于排序和淘汰，相对关系不变。
        def score(r):
            r["rate_mbps"] = round(self._quick_rate(
                r["url"], mode, proxy, bind_ip, threads=PROBE_MEASURE_THREADS), 1)
        if scored:
            with ThreadPoolExecutor(max_workers=len(scored)) as ex:
                list(ex.map(score, scored))
        scored.sort(key=lambda x: -x["rate_mbps"])
        best = scored[0]["rate_mbps"] if scored else 0.0

        alive, dead = [], []
        for r in results:
            if not r["ok"]:
                dead.append(r)
                continue
            if r["rate_mbps"] >= max(floor, best * SOURCE_KEEP_RATIO):
                alive.append(r)
            elif r is scored[0] and r["rate_mbps"] >= 1.0:
                # 兜底：跑得最欢的那个即使没到底限也保留，宁可慢也不要「无源可测」
                r["note"] = "最快源但吞吐偏低，已兜底保留"
                alive.append(r)
            else:
                r["ok"] = False
                r["error"] = "吞吐仅 %.1f Mbps（最优源 %.1f），已剔除" % (
                    r["rate_mbps"], best)
                dead.append(r)

        self._source_cache[mode] = alive
        out = {"mode": mode, "alive": alive, "dead": dead,
               "alive_count": len(alive), "total": len(pool)}
        if self._norm_mode(mode) == "proxy":
            out["proxy"] = self._proxy_report or {}
        return out

    def _get_alive_sources(self, mode: str, proxy_endpoint: str = "") -> list:
        if mode not in self._source_cache or not self._source_cache[mode]:
            self.probe_sources(mode, proxy_endpoint)
        entries = list(self._source_cache.get(mode, []))
        if not entries:
            return []
        if self._norm_mode(mode) == "proxy":
            # 代理线路的瓶颈在节点带宽（几 Mbps~几百 Mbps），单源很容易吃不满，
            # 多源轮换反而更稳；这里只按绝对底限过滤，不做同档淘汰。
            keep = [e for e in entries
                    if (e.get("rate_mbps", 0) or 0) >= PROXY_PROBE_MIN_MBPS]
            return [e["url"] for e in keep] or [entries[0]["url"]]
        # 主源策略：实测迅雷单源 854 Mbps，与腾讯混跑反而掉到 787 ——
        # 慢源不是「多一条路」，而是白占并发名额。只保留与最优源同档的源。
        best = entries[0].get("rate_mbps", 0) or 0
        keep = [e for e in entries
                if (e.get("rate_mbps", 0) or 0) >= best * SOURCE_KEEP_RATIO]
        return [e["url"] for e in keep] or [entries[0]["url"]]

    # -- 网络信息 -----------------------------------------------------------
    def get_network_info(self) -> dict:
        """返回代理配置、TUN 网卡、物理网卡等诊断信息"""
        tun = detect_tun_interfaces()
        info = {
            "system_proxy": self.default_proxy,
            "tun_interfaces": tun,
            "tun_present": len(tun) > 0,
            "physical_ip": detect_physical_ipv4(),
        }
        if self._proxy_report:
            info["proxy_check"] = self._proxy_report
        return info

    # -- 实时吞吐通道 -------------------------------------------------------
    def _live_reset(self, phase: str, duration: float):
        """开始一轮测速前清空实时采样"""
        with self._live_lock:
            self._live.update({
                "prev_bytes": 0, "prev_t": 0.0, "mbps": 0.0, "raw_mbps": 0.0,
                "peak": 0.0, "bytes": 0, "elapsed": 0.0, "phase": phase,
                "ratio": 0.0, "window": [], "samples": [],
            })
            self._t0 = time.perf_counter()
            self._live["_dur"] = max(0.1, float(duration))

    def _live_update(self, byte_count: int, ratio: float):
        """
        用「1 秒滑动窗口」算速率：窗口内首末两个字节计数点之差的字节数
        除以它们的时间差。窗口足够长，能吃掉 Windows socket 缓冲区的
        成批吞吐，不再出现 0.1 秒窗口那种锯齿。算法本身不含任何插值。
        再叠加一层 EMA 低通，让游标连续爬升而不是跳动。
        """
        now = time.perf_counter()
        with self._live_lock:
            lv = self._live
            win = lv["window"]
            win.append((now, byte_count))
            # 只保留最近 LIVE_WINDOW_SECONDS 内的两个及以上的采样点
            while len(win) > 2 and now - win[0][0] > LIVE_WINDOW_SECONDS:
                del win[0]

            lv["bytes"] = byte_count
            lv["elapsed"] = now - self._t0
            lv["ratio"] = ratio

            span = win[-1][0] - win[0][0]
            if span < 0.25:                    # 窗口还没攒够，先不动
                return
            dbytes = max(0, win[-1][1] - win[0][1])
            raw = (dbytes * 8) / (span * 1_000_000)
            lv["raw_mbps"] = round(raw, 1)

            # EMA 低通：首帧直接采纳，之后按系数靠拢
            prev = lv["mbps"]
            lv["mbps"] = round(raw if prev <= 0 else prev + (raw - prev) * LIVE_EMA_ALPHA, 1)

            if lv["mbps"] > lv["peak"]:
                lv["peak"] = lv["mbps"]
            # 预热结束后每秒记一个采样点，作为波形回放数据
            if now - self._t0 >= WARMUP_SECONDS:
                samples = lv["samples"]
                if not samples or lv["elapsed"] - samples[-1][0] >= 0.9:
                    samples.append([round(lv["elapsed"], 2), lv["mbps"]])
                    if len(samples) > 90:
                        del samples[0]

    def get_progress(self) -> dict:
        """前端轮询入口：返回本轮测速的实时速率（全部源自真实字节计数）"""
        with self._live_lock:
            lv = self._live
            return {
                "mbps": lv["mbps"],
                "raw_mbps": lv["raw_mbps"],
                "peak": lv["peak"],
                "bytes": lv["bytes"],
                "elapsed": round(lv["elapsed"], 2),
                "phase": lv["phase"],
                "ratio": round(lv["ratio"], 3),
                "samples": [list(s) for s in lv["samples"]],
            }

    def _live_finish(self, mbps: float):
        with self._live_lock:
            self._live["ratio"] = 1.0
            self._live["mbps"] = round(float(mbps), 1)

    # -- 带宽测试核心 -------------------------------------------------------
    def _download_worker(self, urls: list, worker_idx: int, bind_ip: str,
                         deadline: float, counter: dict, lock: threading.Lock,
                         gate: float = 0.0, health: dict = None,
                         mode: str = "direct", proxy: str = ""):
        """单个下载 worker：多源轮换持续拉取，直到 deadline

        v3.1 两个关键点：
        1. 连接统一走 open_tunnel —— proxy 模式是真的 SOCKS5/HTTP 隧道，
           不再出现「选了代理其实在直连」的假成绩。
        2. 字节计数改为 worker 本地累加、攒够 2MB 或 120ms 才碰一次全局锁。
           旧版每收 256KB 就抢一次锁，48 线程时锁竞争本身就能吃掉上百 Mbps。
        """
        session_idx = 0
        # 同步开闸：所有 worker 一起等到 gate，慢启动落在同一相位
        while gate and time.perf_counter() < gate and not self._cancel.is_set():
            time.sleep(0.02)

        local = [0]                   # 本地未上报字节
        last_flush = [time.perf_counter()]

        def flush():
            if local[0]:
                with lock:
                    counter["bytes"] += local[0]
                local[0] = 0
            last_flush[0] = time.perf_counter()

        while time.perf_counter() < deadline and not self._cancel.is_set():
            url = urls[(worker_idx + session_idx) % len(urls)]
            url = url + ("&" if "?" in url else "?") + "_r=" + str(int(time.time() * 1000)) + \
                str(worker_idx) + str(session_idx)
            session_idx += 1
            sock = None
            sess_bytes = 0
            sess_t0 = 0.0
            follow_url = None
            host = ""
            try:
                u = urlparse(url)
                host = u.hostname
                port = u.port or (443 if u.scheme == "https" else 80)
                path = (u.path or "/") + ("?" + u.query if u.query else "")

                try:
                    # direct 绑定物理网卡绕开 TUN；proxy 走隧道
                    sock = open_tunnel(host, port, mode, proxy, bind_ip, timeout=8.0)
                    sock.settimeout(12.0)
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    try:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCK_RCVBUF)
                    except OSError:
                        pass
                    if u.scheme == "https":
                        ctx = ssl._create_unverified_context()
                        sock = ctx.wrap_socket(sock, server_hostname=host)

                    req = (
                        f"GET {path} HTTP/1.1\r\n"
                        f"Host: {host}\r\n"
                        f"User-Agent: {BROWSER_UA}\r\n"
                        f"Accept: */*\r\n"
                        f"Accept-Language: zh-CN,zh;q=0.9\r\n"
                        f"Connection: close\r\n\r\n"
                    )
                    sock.sendall(req.encode())
                    sess_t0 = time.perf_counter()

                    in_body = False
                    header_buf = b""
                    while time.perf_counter() < deadline and not self._cancel.is_set():
                        chunk = sock.recv(READ_CHUNK)
                        if not chunk:
                            break
                        if not in_body:
                            header_buf += chunk
                            sep = header_buf.find(b"\r\n\r\n")
                            if sep == -1:
                                if len(header_buf) > 65536:
                                    break
                                continue
                            in_body = True
                            # 解析状态行与 Location：3xx 必须跟随，
                            # 否则会把重定向的空 body 当成数据（早期实测里
                            # 腾讯 CDN 被误判成 24.5 Mbps 慢源就是这个原因）。
                            head_txt = header_buf[:sep].decode("latin1", "replace")
                            status_line = head_txt.split("\r\n", 1)[0]
                            parts = status_line.split()
                            status = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
                            if 300 <= status < 400:
                                for line in head_txt.split("\r\n")[1:]:
                                    if line.lower().startswith("location:"):
                                        follow_url = line.split(":", 1)[1].strip()
                                        break
                            body_head = header_buf[sep + 4:]
                            header_buf = b""
                            if body_head and follow_url is None:
                                sess_bytes += len(body_head)
                                local[0] += len(body_head)
                            continue
                        sess_bytes += len(chunk)
                        local[0] += len(chunk)
                        if local[0] >= COUNT_FLUSH_BYTES or \
                                (time.perf_counter() - last_flush[0]) >= COUNT_FLUSH_SECONDS:
                            flush()
                finally:
                    flush()
                    if sock is not None:
                        try:
                            sock.close()
                        except Exception:
                            pass
                # 慢源熔断：单个连接只在约一个滑动窗口内判断，
                # 吞吐低于阈值就登记为慢源，不再接新请求分摊带宽。
                # 注意必须放在跟随重定向判断之前：3xx 的空 body 不是慢，
                # 把重定向误判成慢源正是早期 24.5 Mbps 假慢源的原因。
                if follow_url:
                    # 用真实目标替换源池里的这一条，后续 worker 直接拉取，
                    # 不再每次多花一次 3xx 往返。
                    with lock:
                        for k, item in enumerate(urls):
                            if item == url:
                                urls[k] = follow_url
                                break
                        if health is not None:
                            health.pop(host, None)
                    continue
                if health is not None and sess_t0 and sess_bytes:
                    elapsed = max(0.5, time.perf_counter() - sess_t0)
                    rate = (sess_bytes * 8) / (elapsed * 1_000_000)
                    # 只在「连接活够时长」且「字节确实很少」时判慢源。
                    # 阈值不能只看单连接吞吐：单源多连接时每连接天然分摊，
                    # 真正的慢源特征是「远低于任何合理单连接水平」（<8 Mbps）。
                    if elapsed >= 1.0 and sess_bytes < 4 * 1024 * 1024 and \
                            rate < DOWNLOAD_MIN_SOURCE_MBPS:
                        with lock:
                            health[host] = rate
            except Exception:
                time.sleep(0.05)

    def test_bandwidth(self, direction: str = "download", mode: str = "direct",
                       proxy_endpoint: str = "", threads: int = DEFAULT_THREADS,
                       duration: int = DEFAULT_DURATION) -> dict:
        """
        原生带宽测试（有效传输窗口计时）
        返回: {mbps, bytes, duration, sources_used, threads, bind_ip, proxy, note}
        """
        self._cancel.clear()
        # 千兆链路的平台期在 32~48 并发；低于 24 会因为单连接限速直接塌到一半。
        threads = max(1, min(int(threads or DEFAULT_THREADS), MAX_THREADS))
        duration = max(5, min(int(duration or DEFAULT_DURATION), 30))

        is_proxy = self._norm_mode(mode) == "proxy"
        proxy = self._resolve_proxy(proxy_endpoint) if is_proxy else ""
        if is_proxy:
            # 代理隧道里每个连接都要在代理端做一次转发，并发过高温和反降：
            # 实测 32 线程 260 Mbps、48 线程 134 Mbps。所以代理侧封顶 32。
            threads = min(threads, 32)
        proxy_info = {}
        if is_proxy:
            # 硬校验：代理确实可用且出口 IP 与直连不同，才允许开测。
            # 旧版在这里静默直连，用户看到的是一个「海外测速」的国内成绩。
            chk = self.check_proxy(proxy)
            proxy_info = chk
            if not chk.get("ok"):
                return {"mbps": 0.0, "bytes": 0, "proxy": chk,
                        "error": "代理不可用：%s" % (chk.get("error") or "未知原因")}
            if not chk.get("via_proxy"):
                return {"mbps": 0.0, "bytes": 0, "proxy": chk,
                        "error": "代理出口与直连相同（%s），流量未经过代理线路"
                                 % chk.get("exit_ip", "")}

        if direction == "upload":
            self._live_reset("upload", duration)
            res = self._upload_test(threads, duration, self._norm_mode(mode), proxy)
            if proxy_info:
                res["proxy"] = proxy_info
            return res

        urls = self._get_alive_sources(mode, proxy_endpoint)
        if not urls:
            err = "无可用测速源（预检全部失败）"
            if is_proxy:
                chk = self._proxy_report or {}
                if chk.get("error"):
                    err = "代理链路不可用：%s" % chk["error"]
            return {"mbps": 0.0, "bytes": 0, "error": err, "proxy": proxy_info}

        bind_ip = ""
        note = ""
        if not is_proxy:
            # 直连模式：优先绑定物理网卡，绕开 TUN
            bind_ip = detect_physical_ipv4()
            if detect_tun_interfaces() and bind_ip:
                note = f"已绑定物理网卡 {bind_ip}，绕过 TUN 虚拟网卡"
        else:
            note = "经代理隧道 %s（出口 %s / %s）" % (
                proxy, proxy_info.get("exit_ip", "?"), proxy_info.get("exit_loc", "?"))

        counter = {"bytes": 0}
        lock = threading.Lock()
        health = {}                      # host -> 实测 Mbps，慢源登记表
        self._live_reset("download", duration)
        start = time.perf_counter()
        deadline = start + duration
        # 预热结束后再留 SYNC_PHASE_SECONDS 的同步相位：
        # 让所有 worker 在 gate 同一时刻真正开始冲流量，
        # 稳态段不会被「先到先跑」的慢启动尾巴污染。
        gate = start + WARMUP_SECONDS
        base_t = gate + SYNC_PHASE_SECONDS
        if base_t > deadline:
            base_t = deadline

        def worker(i):
            self._download_worker(urls, i, bind_ip, deadline, counter, lock, gate,
                                  health, mode=self._norm_mode(mode), proxy=proxy)

        with ThreadPoolExecutor(max_workers=threads) as pool:
            futures = [pool.submit(worker, i) for i in range(threads)]
            # 预热期间也把字节数喂给实时通道（但不计入稳态），
            # 这样前端从第 1 秒起看到的是真实爬升而不是空白。
            while time.perf_counter() < base_t and not self._cancel.is_set():
                with lock:
                    cur = counter["bytes"]
                self._live_update(cur, 0.0)
                time.sleep(0.1)
            # 预热期字节在这里才真正清零：基准点取在同步相位结束之后，
            # 慢启动与 TLS 握手产生的字节不会再被算进稳态平均值。
            with lock:
                base_bytes = counter["bytes"]
            base_t = time.perf_counter()
            # 稳态段继续实时上报速率
            while any(not f.done() for f in futures) and not self._cancel.is_set():
                with lock:
                    cur = counter["bytes"]
                self._live_update(cur, 0.0)
                time.sleep(0.1)
            for f in futures:
                f.result()

        end = time.perf_counter()
        steady_bytes = counter["bytes"] - base_bytes
        steady_sec = max(0.1, end - base_t)
        mbps = (steady_bytes * 8) / (steady_sec * 1_000_000)
        self._live_finish(mbps)

        # 收尾时把实时值对齐到稳态均值之前，先用稳态段均值重算一次
        # 游标终点，避免「峰值锁回」的视觉落差。
        slow = {h: round(r, 1) for h, r in health.items()}

        return {
            "mbps": round(mbps, 1),
            "bytes": counter["bytes"],
            "steady_bytes": steady_bytes,
            "steady_sec": round(steady_sec, 2),
            "threads": threads,
            "bind_ip": bind_ip,
            "sources_used": len(urls),
            "slow_sources": slow,
            "proxy": proxy_info,
            "note": note,
        }

    def _upload_test(self, threads: int, duration: int, mode: str = "direct",
                     proxy: str = "") -> dict:
        """上传测速（固定载荷，按有效窗口统计），proxy 模式同样走隧道"""
        payload = b"0" * (512 * 1024)
        counter = {"bytes": 0}
        lock = threading.Lock()
        start = time.perf_counter()
        deadline = start + duration
        gate = start + WARMUP_SECONDS
        base_t = gate + SYNC_PHASE_SECONDS
        if base_t > deadline:
            base_t = deadline
        url = UPLOAD_SOURCES[0]
        u = urlparse(url)

        def worker():
            # 与下载侧一致：所有 worker 在 gate 同一时刻开始冲流量
            while time.perf_counter() < gate and not self._cancel.is_set():
                time.sleep(0.02)
            while time.perf_counter() < deadline and not self._cancel.is_set():
                try:
                    host = u.hostname
                    port = u.port or 443
                    ctx = ssl._create_unverified_context()
                    sock = open_tunnel(host, port, mode, proxy, timeout=8.0)
                    sock.settimeout(12.0)
                    sock = ctx.wrap_socket(sock, server_hostname=host)
                    req = (
                        f"POST {u.path} HTTP/1.1\r\n"
                        f"Host: {host}\r\n"
                        f"User-Agent: {BROWSER_UA}\r\n"
                        f"Content-Type: application/octet-stream\r\n"
                        f"Content-Length: {len(payload)}\r\n"
                        f"Connection: close\r\n\r\n"
                    )
                    sock.sendall(req.encode())
                    sent = 0
                    while sent < len(payload) and time.perf_counter() < deadline:
                        s = sock.send(payload[sent:sent + 65536])
                        sent += s
                        with lock:
                            counter["bytes"] += s
                    try:
                        sock.recv(4096)
                    except Exception:
                        pass
                    sock.close()
                except Exception:
                    time.sleep(0.1)

        with ThreadPoolExecutor(max_workers=threads) as pool:
            futures = [pool.submit(worker) for _ in range(threads)]
            while time.perf_counter() < base_t and not self._cancel.is_set():
                with lock:
                    cur = counter["bytes"]
                self._live_update(cur, 0.0)
                time.sleep(0.1)
            with lock:
                base = counter["bytes"]
            base_t = time.perf_counter()
            while any(not f.done() for f in futures) and not self._cancel.is_set():
                with lock:
                    cur = counter["bytes"]
                self._live_update(cur, 0.0)
                time.sleep(0.1)
            for f in futures:
                f.result()
        end = time.perf_counter()
        steady = counter["bytes"] - base
        secs = max(0.1, end - base_t)
        mbps = (steady * 8) / (secs * 1_000_000)
        self._live_finish(mbps)
        return {"mbps": round(mbps, 1),
                "bytes": counter["bytes"], "steady_bytes": steady,
                "steady_sec": round(secs, 2), "threads": threads}

    def cancel_test(self) -> dict:
        self._cancel.set()
        with self._live_lock:
            self._live["phase"] = "idle"
        return {"cancelled": True}

    # -- 延迟探测 -----------------------------------------------------------
    def _http_ttfb(self, target: str, path: str, mode: str, proxy: str,
                   bind_ip: str = "", timeout: float = 6.0):
        """建隧道+TLS 后发一次 HTTP 请求，返回 (首字节耗时ms, 建连耗时ms)

        首字节耗时的计时起点在 TLS 握手**之后**，所以它只包含：
        请求上行 1 RTT + 服务端首字节处理 —— 这才是用户感知的「延迟」。
        """
        t_conn = time.perf_counter()
        s = open_tunnel(target, 443, mode, proxy, bind_ip, timeout=timeout)
        try:
            s.settimeout(timeout)
            s = ssl._create_unverified_context().wrap_socket(s, server_hostname=target)
            connect_ms = (time.perf_counter() - t_conn) * 1000
            req = (f"GET {path} HTTP/1.1\r\nHost: {target}\r\n"
                   f"User-Agent: {BROWSER_UA}\r\nAccept: */*\r\n"
                   f"Accept-Language: zh-CN,zh;q=0.9\r\n"
                   f"Connection: close\r\n\r\n")
            t0 = time.perf_counter()
            s.sendall(req.encode())
            first = s.recv(64)
            rtt_ms = (time.perf_counter() - t0) * 1000
            if not first:
                raise OSError("服务端未返回数据")
            return rtt_ms, connect_ms
        finally:
            try:
                s.close()
            except Exception:
                pass

    def _http_rtt_samples(self, target: str, path: str, mode: str, proxy: str,
                          bind_ip: str = "", samples: int = 3,
                          timeout: float = 6.0):
        """
        在**同一条** keep-alive 连接上连发多次请求，逐个记首字节 RTT。

        为什么要复用连接：每次重建都要走 SOCKS 建链 + TLS，实测 connect_ms
        在代理下能在 280ms~1200ms 之间乱跳，混进延迟里就成了「走 VPN 延迟很高」
        的假象。复用之后得到的才是纯链路往返。
        返回 (rtt_list, connect_ms)
        """
        t_conn = time.perf_counter()
        s = open_tunnel(target, 443, mode, proxy, bind_ip, timeout=timeout)
        try:
            s.settimeout(timeout)
            s = ssl._create_unverified_context().wrap_socket(s, server_hostname=target)
            connect_ms = (time.perf_counter() - t_conn) * 1000
            req = (f"GET {path} HTTP/1.1\r\nHost: {target}\r\n"
                   f"User-Agent: {BROWSER_UA}\r\nAccept: */*\r\n"
                   f"Accept-Language: zh-CN,zh;q=0.9\r\n"
                   f"Connection: keep-alive\r\n\r\n").encode()
            rtts = []
            reusable = True
            for _ in range(max(1, min(int(samples), 6))):
                if not reusable:
                    break
                t0 = time.perf_counter()
                s.sendall(req)
                buf = b""
                while b"\r\n\r\n" not in buf:
                    c = s.recv(8192)
                    if not c:
                        raise OSError("连接被对端关闭")
                    buf += c
                rtts.append((time.perf_counter() - t0) * 1000)

                head, _, body = buf.partition(b"\r\n\r\n")
                head_txt = head.decode("latin1", "replace")
                low = head_txt.lower()
                if "connection: close" in low or b"connection: close" in head.lower():
                    reusable = False
                length = None
                for line in head_txt.split("\r\n")[1:]:
                    if line.lower().startswith("content-length:"):
                        try:
                            length = int(line.split(":", 1)[1].strip())
                        except ValueError:
                            length = None
                if length is not None:
                    got = len(body)
                    while got < length:
                        c = s.recv(min(65536, length - got))
                        if not c:
                            reusable = False
                            break
                        got += len(c)
                elif "transfer-encoding" in low and "chunked" in low:
                    # chunked 没有长度，读到结束标记为止（带总量上限防跑飞）
                    acc = body
                    while b"0\r\n\r\n" not in acc and len(acc) < 2 * 1024 * 1024:
                        c = s.recv(16384)
                        if not c:
                            reusable = False
                            break
                        acc += c
                    if b"0\r\n\r\n" not in acc:
                        reusable = False
                else:
                    # 无长度信息（或 204/304 无 body）：连接状态不可靠，不再复用
                    reusable = False
            return rtts, connect_ms
        finally:
            try:
                s.close()
            except Exception:
                pass

    def ping_target(self, url: str, host: str = "", mode: str = "direct",
                    proxy_endpoint: str = "", samples: int = 3) -> dict:
        """
        延迟口径 v3.2.2（实测校准）：

        测什么：建好隧道 + TLS 后，在同一条 keep-alive 连接上 GET `/favicon.ico`，
        从发请求到收到首字节的时间。多次采样取最小值。
        为什么不是首页：实测走代理首页 TTFB 371~481ms（含服务端动态渲染），
        同站 /favicon.ico 只要 76~96ms —— 前者测「网站快不快」，后者才是「网络延迟」。
        为什么不是 TLS 握手：含建链与多次往返，实测 250~1600ms，虚高。
        """
        mode = self._norm_mode(mode)
        proxy = self._resolve_proxy(proxy_endpoint) if mode == "proxy" else ""
        bind_ip = detect_physical_ipv4() if mode != "proxy" else ""
        try:
            u = urlparse(url)
            target = u.hostname or host
            port = u.port or (443 if u.scheme == "https" else 80)

            if u.scheme != "https":
                t0 = time.perf_counter()
                s = open_tunnel(target, port, mode, proxy, bind_ip, timeout=4.0)
                s.close()
                ms = int((time.perf_counter() - t0) * 1000)
                return {"latency_ms": max(1, ms), "status": "ok",
                        "route": "proxy" if mode == "proxy" else "direct",
                        "basis": "connect", "samples": [ms], "connect_ms": ms}

            paths = ["/favicon.ico", (u.path or "/")]
            rtts, connect_ms = [], 0.0
            last_err = None
            for p in paths:
                try:
                    rtts, connect_ms = self._http_rtt_samples(
                        target, p, mode, proxy, bind_ip, samples=samples)
                    if rtts:
                        break
                except Exception as e:
                    last_err = e
            if not rtts:
                raise last_err or OSError("多次采样均未取到首字节")

            return {
                "latency_ms": max(1, int(round(min(rtts)))),
                "median_ms": int(round(sorted(rtts)[len(rtts) // 2])),
                "samples": [int(round(x)) for x in rtts],
                "connect_ms": int(round(connect_ms)),
                "status": "ok",
                "route": "proxy" if mode == "proxy" else "direct",
                "basis": "http_rtt_keepalive",
            }
        except Exception as e:
            return {"latency_ms": 999, "status": "timeout", "error": str(e)[:80]}

    def latency_stats(self, url: str, mode: str = "direct",
                      proxy_endpoint: str = "", samples: int = 3) -> dict:
        """
        仪表盘的 Ping / Jitter：两次独立建连、每次内部多次 RTT 采样。
        ping 取全部样本最小值（线路能做到多低），jitter 用中位绝对偏差
        （比相邻差更抗单次异常值）。前端原先的 62 / 14 是硬编码常量。
        """
        vals, conns, attempts = [], [], 0
        for _ in range(2):
            attempts += 1
            r = self.ping_target(url, "", mode, proxy_endpoint, samples=samples)
            if r.get("status") == "ok":
                vals.extend(r.get("samples") or [int(r["latency_ms"])])
                conns.append(int(r.get("connect_ms", 0)))
        if not vals:
            return {"ping": 999, "jitter": 0.0, "samples": [], "status": "timeout",
                    "loss": 100.0, "attempts": attempts}

        ping = min(vals)
        sv = sorted(vals)
        med = sv[len(sv) // 2]
        mad = sorted(abs(v - med) for v in vals)[len(vals) // 2]
        return {
            "ping": int(round(ping)),
            "median": int(round(med)),
            "jitter": round(float(mad), 1),
            "samples": [int(round(v)) for v in vals],
            "connect_ms": int(round(sum(conns) / len(conns))) if conns else 0,
            "loss": round((attempts - len(conns)) * 100.0 / attempts, 1),
            "attempts": attempts,
            "status": "ok",
        }


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # PyInstaller onefile 解包后资源在 _MEIPASS
    candidates = [
        os.path.join(getattr(sys, "_MEIPASS", base_dir), "netpulse_speedtest.html"),
        os.path.join(base_dir, "netpulse_speedtest.html"),
    ]
    html_path = next((p for p in candidates if os.path.exists(p)), None)
    if not html_path:
        print("[错误] 未找到前端页面 netpulse_speedtest.html")
        sys.exit(1)

    api = AeroPulseDesktopAPI()

    if webview is None:
        import webbrowser
        print("[提示] 未检测到 pywebview，使用系统浏览器打开界面")
        webbrowser.open("file:///" + html_path.replace(os.sep, "/"))
        return

    # 窗口图标：pywebview 的 winforms/edgechromium 后端用 ico 文件设置任务栏与标题栏
    icon_path = os.path.join(base_dir, "icon_src", "aeropulse.ico")
    if not os.path.exists(icon_path):
        # PyInstaller 打包后从 _MEIPASS 找
        icon_path = os.path.join(getattr(sys, "_MEIPASS", base_dir), "icon_src", "aeropulse.ico")

    webview.create_window(
        title="Dual-use · Windows 双模网络测速与分流延迟诊断终端 v3.0",
        url=html_path,
        js_api=api,
        width=1280,
        height=840,
        min_size=(980, 680),
        background_color="#F4F6F9",
        text_select=False,
    )
    webview.start(debug=False, icon=icon_path if os.path.exists(icon_path) else None)


if __name__ == "__main__":
    main()
