import asyncio
import aiohttp
from datetime import datetime
from typing import Dict
from utils.logger import logger
from config import config
from exchanges.upbit_client import UpbitClient
from exchanges.bitget_client import BitgetClient
from exchanges.bithumb_client import BithumbClient
from exchanges.kis_client import KISClient

class AssetMonitor:
    def _clamp(self, text: str, limit: int = 900) -> str:
        """Embed 필드 길이를 초과하지 않도록 자르는 유틸"""
        return text if len(text) <= limit else text[: limit - 3] + '...'

    def __init__(self):
        self.upbit = UpbitClient()
        self.bitget_demo = BitgetClient(use_demo=True)
        self.bitget_real = BitgetClient(use_demo=False)
        self.bithumb = BithumbClient()
        self.kis_clients = {}

        # KIS 클라이언트는 토큰 재활용을 위해 미리 생성
        for account_name, info in config.KIS_ACCOUNTS.items():
            self.kis_clients[account_name] = KISClient(
                key=info.get("key"),
                secret=info.get("secret"),
                account_number=info.get("account_number"),
                account_code=info.get("account_code"),
                kis_number=int(account_name.replace("KIS", "")) if account_name.replace("KIS", "").isdigit() else 0,
            )
        
    async def get_crypto_assets(self) -> Dict:
        """암호화폐 자산 조회"""
        assets: Dict = {}

        def set_error(name: str, err: Exception | str):
            assets[name] = {"error": str(err)}

        # Upbit 자산 조회
        try:
            upbit_balance = self.upbit.get_balance()
            if isinstance(upbit_balance, dict) and upbit_balance:
                total_krw = float(upbit_balance.get('KRW', {}).get('total', 0) or 0)
                crypto_balances = {
                    k: v.get('total', 0)
                    for k, v in upbit_balance.items()
                    if k != 'KRW' and isinstance(v, dict) and v.get('total', 0) > 0
                }
                assets['UPBIT'] = {
                    'total_krw': total_krw,
                    'crypto_balances': crypto_balances
                }
            else:
                set_error('UPBIT', '잔고 조회 결과 없음')
        except Exception as e:
            logger.error(f"Upbit 자산 조회 오류: {e}")
            set_error('UPBIT', e)
        
        # Bitget Demo 자산 조회
        try:
            bitget_demo_balance = self.bitget_demo.get_balance()
            if isinstance(bitget_demo_balance, dict) and bitget_demo_balance:
                total_usdt = bitget_demo_balance.get('USDT', {}).get('total', 0)
                crypto_balances = {k: v['total'] for k, v in bitget_demo_balance.items() 
                                if k != 'USDT' and isinstance(v, dict) and v.get('total', 0) > 0}
                assets['BITGET_DEMO'] = {
                    'total_usdt': total_usdt,
                    'crypto_balances': crypto_balances
                }
        except Exception as e:
            logger.error(f"Bitget Demo 자산 조회 오류: {e}")
            set_error('BITGET_DEMO', e)
            
        # Bitget Real 자산 조회
        try:
            bitget_real_balance = self.bitget_real.get_balance()
            if isinstance(bitget_real_balance, dict) and bitget_real_balance:
                total_usdt = bitget_real_balance.get('USDT', {}).get('total', 0)
                crypto_balances = {k: v['total'] for k, v in bitget_real_balance.items() 
                                if k != 'USDT' and isinstance(v, dict) and v.get('total', 0) > 0}
                assets['BITGET_REAL'] = {
                    'total_usdt': total_usdt,
                    'crypto_balances': crypto_balances
                }
        except Exception as e:
            logger.error(f"Bitget Real 자산 조회 오류: {e}")
            set_error('BITGET_REAL', e)
        
        # Bithumb 자산 조회
        try:
            bithumb_balance = self.bithumb.get_balance()
            if isinstance(bithumb_balance, dict) and bithumb_balance:
                total_krw = bithumb_balance.get('KRW', {}).get('total', 0)
                crypto_balances = {k: v['total'] for k, v in bithumb_balance.items() 
                                if k != 'KRW' and isinstance(v, dict) and v.get('total', 0) > 0}
                assets['BITHUMB'] = {
                    'total_krw': total_krw,
                    'crypto_balances': crypto_balances
                }
            else:
                set_error('BITHUMB', '잔고 조회 결과 없음')
        except Exception as e:
            logger.error(f"Bithumb 자산 조회 오류: {e}")
            set_error('BITHUMB', e)
        
        return assets
    
    async def get_stock_assets(self) -> Dict:
        """주식 자산 조회 (KIS 계좌들)"""
        stock_assets: Dict = {}
        items = list(self.kis_clients.items())
        for idx, (account_name, client) in enumerate(items):
            try:
                stock_assets[account_name] = client.get_balance()
            except Exception as e:
                logger.error(f"{account_name} 주식 자산 조회 오류: {e}")
                stock_assets[account_name] = {"error": str(e)}
            # KIS API 속도 제한: 계좌마다 5초 간격
            if idx < len(items) - 1:
                await asyncio.sleep(5)
        return stock_assets
    
    def format_asset_message(self, crypto_assets: Dict, stock_assets: Dict) -> Dict:
        """자산 현황을 디스코드 메시지 형식으로 포맷"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        embed = {
            "title": "💰 자동매매봇 자산 현황",
            "description": f"조회 시간: {timestamp}",
            "color": 0x00ff00,
            "fields": []
        }
        
        # 암호화폐 자산
        if crypto_assets:
            crypto_text = ""
            for exchange, data in crypto_assets.items():
                crypto_text += f"**{exchange}**\n"
                if data.get("error"):
                    crypto_text += f"에러: {data['error']}\n\n"
                    continue
                if 'total_krw' in data:
                    crypto_text += f"현금: {data['total_krw']:,.0f} KRW\n"
                elif 'total_usdt' in data:
                    crypto_text += f"현금: {data['total_usdt']:,.2f} USDT\n"
                
                if data.get('crypto_balances'):
                    crypto_text += "보유 코인:\n"
                    for coin, amount in list(data['crypto_balances'].items())[:5]:
                        crypto_text += f"  • {coin}: {amount:g}\n"
                crypto_text += "\n"
            
            embed["fields"].append({
                "name": "🪙 암호화폐 거래소",
                "value": self._clamp(crypto_text),
                "inline": False
            })
        
        # 주식 자산 (KIS)
        if stock_assets:
            total_dom_value = 0
            total_dom_pnl = 0
            total_ovrs_value = 0
            total_futures_pnl_krw = 0
            total_combined = 0
            usd_rate = None
            account_fields = []
            any_futures_enabled = False

            for account, data in stock_assets.items():
                if data.get("error"):
                    account_fields.append({
                        "name": f"📈 {account}",
                        "value": f"조회 실패: {data['error']}",
                        "inline": False,
                    })
                    continue

                dom = data.get("domestic", {})
                ovrs = data.get("overseas", {})
                fut = data.get("futures", {})
                fx_rates = ovrs.get("fx_rates") or {}
                if usd_rate is None:
                    usd_candidate = fx_rates.get("USD")
                    if usd_candidate:
                        usd_rate = usd_candidate

                dom_total = dom.get("total_krw", 0) or 0
                ovrs_total = ovrs.get("total_krw", 0) or 0
                acc_total = dom_total + ovrs_total
                acc_pnl = data.get("pnl_krw", dom.get("pnl_krw", 0))
                acc_rate = data.get("pnl_rate", dom.get("pnl_rate", 0))
                cash = dom.get("cash")
                cash_text = f", 현금 {cash:,.0f}원" if cash is not None else ""
                futures_disabled = fut.get("disabled")

                total_dom_value += dom_total
                total_dom_pnl += acc_pnl or 0
                total_ovrs_value += ovrs_total
                total_combined += acc_total
                if not futures_disabled:
                    total_futures_pnl_krw += fut.get("total_pnl_krw", 0) or 0
                    any_futures_enabled = True

                lines = []
                lines.append(
                    f"총 평가 {acc_total:,.0f}원 (국내 {dom_total:,.0f}원, 해외 {ovrs_total:,.0f}원)"
                    f" / 손익 {acc_pnl:,.0f}원 ({acc_rate:+.2f}%)"
                    f"{cash_text}"
                )
                if usd_rate:
                    lines.append(
                        f"→ USD 환산 총평가 ≈ ${acc_total / usd_rate:,.2f} (기준 {usd_rate:,.2f} KRW/USD)"
                    )

                dom_stocks = dom.get("stocks", [])
                if dom_stocks:
                    lines.append("국내 상위 3개:")
                    for s in dom_stocks[:3]:
                        lines.append(
                            f"  • {s.get('name','')}({s.get('symbol','')}): {s.get('quantity',0):g}주, "
                            f"평가 {s.get('eval_amount',0):,.0f}원, 손익 {s.get('pnl',0):,.0f}원 ({s.get('pnl_rate',0):+.2f}%)"
                        )
                    if len(dom_stocks) > 3:
                        lines.append(f"  • 외 {len(dom_stocks) - 3}종목 보유")

                ovrs_stocks = ovrs.get("stocks", [])
                totals = ovrs.get("per_currency", {})
                fx_rates = ovrs.get("fx_rates") or {}
                fx_meta = ovrs.get("fx_meta") or {}
                if ovrs_stocks or totals:
                    lines.append("해외 잔고:")
                    for cur, vals in totals.items():
                        lines.append(
                            f"  • {cur}: 평가 {vals.get('total_eval',0):,.2f}, 손익 {vals.get('total_pnl',0):,.2f}"
                        )
                    if ovrs_stocks:
                        lines.append("해외 상위 3개:")
                        top_ovrs = sorted(ovrs_stocks, key=lambda s: s.get('eval_amount', 0), reverse=True)[:3]
                        for s in top_ovrs:
                            lines.append(
                                f"  • {s.get('name','')}({s.get('symbol','')}) {s.get('exchange','')}/{s.get('currency','')}: {s.get('quantity',0):g}, "
                                f"평가 {s.get('eval_amount',0):,.2f}, 손익 {s.get('pnl',0):,.2f} ({s.get('pnl_rate',0):+.2f}%)"
                            )
                        if len(ovrs_stocks) > 3:
                            lines.append(f"  • 외 {len(ovrs_stocks) - 3}종목 보유")

                    if fx_rates:
                        preferred = ["USD", "HKD", "JPY"]
                        rate_parts = []
                        for cur in preferred:
                            rate = fx_rates.get(cur)
                            if rate:
                                rate_parts.append(f"{cur} {rate:,.2f}")
                        for cur, rate in fx_rates.items():
                            if cur in preferred:
                                continue
                            if rate and len(rate_parts) < 5:
                                rate_parts.append(f"{cur} {rate:,.2f}")
                        if rate_parts:
                            source = fx_meta.get("source") or "N/A"
                            cache_note = " (cache)" if fx_meta.get("from_cache") else ""
                            lines.append(f"환율[{source}{cache_note}]: " + ", ".join(rate_parts))

                # 해외선물/옵션
                if fut:
                    if fut.get("disabled"):
                        lines.append("해외선물/옵션 조회 중지됨")
                        account_fields.append({
                            "name": f"📈 {account}",
                            "value": self._clamp("\n".join(lines)),
                            "inline": False,
                        })
                        continue
                    fut_error = fut.get("error")
                    if fut_error:
                        lines.append(f"해외선물/옵션 조회 실패: {fut_error}")
                    else:
                        fut_totals = fut.get("per_currency", {})
                        fut_positions = fut.get("positions", [])
                        if fut_totals or fut_positions:
                            lines.append("해외선물/옵션:")
                        for cur, vals in fut_totals.items():
                            pnl = vals.get("pnl", 0)
                            pnl_krw = vals.get("pnl_krw")
                            pnl_krw_text = f", 원화 {pnl_krw:,.0f}원" if pnl_krw is not None else ""
                            lines.append(f"  • {cur}: 손익 {pnl:,.2f}{pnl_krw_text}")
                        if fut.get("total_pnl_krw"):
                            lines.append(f"  • 합산 손익(원화): {fut['total_pnl_krw']:,.0f}원")
                        if fut_positions:
                            lines.append("  포지션 상위 3개:")
                            for p in fut_positions[:3]:
                                lines.append(
                                    f"    • {p.get('symbol','')} {p.get('currency','')}/{p.get('side','')}: {p.get('quantity',0):g}, "
                                    f"가격 {p.get('current_price',0):,.2f}, 손익 {p.get('pnl',0):,.2f}"
                                )
                            if len(fut_positions) > 3:
                                lines.append(f"    • 외 {len(fut_positions) - 3}포지션 보유")

                account_fields.append({
                    "name": f"📈 {account}",
                    "value": self._clamp("\n".join(lines)),
                    "inline": False,
                })

            summary_lines = [
                f"국내 합계 {total_dom_value:,.0f}원 (손익 {total_dom_pnl:,.0f}원)",
                f"해외 합계 {total_ovrs_value:,.0f}원"
                + (f" (≈ ${total_ovrs_value / usd_rate:,.2f})" if usd_rate else ""),
            ]
            futures_note = (
                f" / 해외선물 손익(원화) {total_futures_pnl_krw:,.0f}원"
                if any_futures_enabled
                else " / 해외선물 조회 중지됨"
            )
            summary_lines.append(
                f"총 평가 {total_combined:,.0f}원{futures_note}"
                + (f" / USD 환산 ≈ ${total_combined / usd_rate:,.2f}" if usd_rate else "")
            )
            embed["fields"].append({
                "name": "요약",
                "value": "\n".join(summary_lines),
                "inline": False,
            })
            embed["fields"].extend(account_fields)
        
        return {"embeds": [embed]}
    
    async def send_discord_message(self, message: Dict):
        """디스코드 웹훅으로 메시지 전송"""
        if not config.DISCORD_WEBHOOK_URL:
            logger.info("디스코드 웹훅 URL이 설정되지 않았습니다.")
            return
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(config.DISCORD_WEBHOOK_URL, json=message) as response:
                    if response.status == 204:
                        logger.info("디스코드 자산 리포트 전송 완료")
                    else:
                        body = await response.text()
                        logger.error(f"디스코드 메시지 전송 실패: {response.status} - {body[:300]}")
        except Exception as e:
            logger.error(f"디스코드 메시지 전송 오류: {e}")
    
    async def generate_asset_report(self):
        """자산 현황 리포트 생성 및 전송"""
        try:
            logger.info("자산 현황 조회 시작")
            
            crypto_assets = await self.get_crypto_assets()
            stock_assets = await self.get_stock_assets()
            
            if crypto_assets or stock_assets:
                message = self.format_asset_message(crypto_assets, stock_assets)
                await self.send_discord_message(message)
                logger.info("자산 리포트 생성 완료")
            else:
                logger.warning("조회된 자산이 없습니다")
                
        except Exception as e:
            logger.error(f"자산 리포트 생성 오류: {e}")

async def run_asset_monitoring():
    """자산 모니터링 메인 루프"""
    monitor = AssetMonitor()
    
    while True:
        try:
            await monitor.generate_asset_report()
            # 6시간(21600초) 대기
            await asyncio.sleep(config.RESTART_INTERVAL_HOURS * 3600)
        except Exception as e:
            logger.error(f"자산 모니터링 오류: {e}")
            await asyncio.sleep(300)  # 5분 후 재시도

if __name__ == "__main__":
    asyncio.run(run_asset_monitoring())
