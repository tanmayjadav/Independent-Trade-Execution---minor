# reporting/session_reporter.py

from datetime import datetime
import csv
import os
from reporting.discord import DiscordAlert


class SessionReporter:
    """
    Tracks intraday performance and sends EOD report to Discord.
    """

    def __init__(self, webhook_url: str, trade_repo=None):
        self.webhook_url = webhook_url
        self.discord = DiscordAlert()
        self.trade_repo = trade_repo

        # Trade stats
        self.total_trades = 0
        self.wins = 0
        self.losses = 0
        self.net_pnl = 0.0
        
        # Detailed PnL tracking for advanced metrics
        self.win_pnls = []  # List of positive PnLs
        self.loss_pnls = []  # List of negative PnLs (absolute values)

        # Drawdown tracking
        self.equity_curve = [0.0]
        self.max_drawdown = 0.0

    # -------------------------------------------------
    # Trade lifecycle hooks
    # -------------------------------------------------

    def on_trade_closed(self, pnl: float):
        """
        Call this whenever a position is closed.
        """

        self.total_trades += 1
        self.net_pnl += pnl

        if pnl > 0:
            self.wins += 1
            self.win_pnls.append(pnl)
        elif pnl < 0:
            self.losses += 1
            self.loss_pnls.append(abs(pnl))

        # Update equity curve
        current_equity = self.equity_curve[-1] + pnl
        self.equity_curve.append(current_equity)

        self._update_drawdown()

    # -------------------------------------------------
    # Drawdown logic
    # -------------------------------------------------

    def _update_drawdown(self):
        peak = max(self.equity_curve)
        trough = self.equity_curve[-1]
        drawdown = peak - trough
        self.max_drawdown = max(self.max_drawdown, drawdown)

    # -------------------------------------------------
    # Final report
    # -------------------------------------------------

    def send_eod_report(self):
        """
        Sends end-of-day summary to Discord and exports CSV report.
        """

        # Calculate win/loss ratio
        win_loss_ratio = (
            round(self.wins / self.losses, 2)
            if self.losses > 0 else ("∞" if self.wins > 0 else 0.0)
        )
        
        # Calculate win rate
        win_rate = (
            round((self.wins / self.total_trades) * 100, 2)
            if self.total_trades > 0 else 0.0
        )
        
        # Calculate average win and loss
        avg_win = round(sum(self.win_pnls) / len(self.win_pnls), 2) if self.win_pnls else 0.0
        avg_loss = round(sum(self.loss_pnls) / len(self.loss_pnls), 2) if self.loss_pnls else 0.0
        
        # Calculate profit factor
        total_wins = sum(self.win_pnls)
        total_losses = sum(self.loss_pnls)
        profit_factor = (
            round(total_wins / total_losses, 2)
            if total_losses > 0 else ("∞" if total_wins > 0 else 0.0)
        )
        
        # Max win and loss
        max_win = round(max(self.win_pnls), 2) if self.win_pnls else 0.0
        max_loss = round(max(self.loss_pnls), 2) if self.loss_pnls else 0.0

        # Get total trade records (ENTRY + EXIT) for reference
        total_trade_records = 0
        if self.trade_repo:
            try:
                today = datetime.now().date()
                all_trades = self.trade_repo.get_date_trades(today)
                total_trade_records = len(all_trades) if all_trades else 0
            except:
                pass
        
        message = {
            "title": "NIFTY EMA – End of Day Report",
            "date": datetime.now().strftime("%d %b %Y"),
            "completed trades": self.total_trades,  # Closed positions (EXIT trades only)
            "total trade records": total_trade_records if total_trade_records > 0 else None,  # All fills (ENTRY + EXIT)
            "wins": self.wins,
            "losses": self.losses,
            "win rate (%)": win_rate,
            "win/loss ratio": win_loss_ratio,
            "net pnl (₹)": round(self.net_pnl, 2),
            "avg win (₹)": avg_win,
            "avg loss (₹)": avg_loss,
            "profit factor": profit_factor,
            "max win (₹)": max_win,
            "max loss (₹)": max_loss,
            "max drawdown (₹)": round(self.max_drawdown, 2),
            "color": "green" if self.net_pnl >= 0 else "red",
        }

        self.discord.send_alert(
            webhook_url=self.webhook_url,
            message=message,
            use_embed=True
        )

        # Save daily summary to database
        if self.trade_repo:
            try:
                today = datetime.now().date()
                self.trade_repo.save_daily_summary(
                    date=today,
                    total_trades=self.total_trades,
                    wins=self.wins,
                    losses=self.losses,
                    net_pnl=self.net_pnl,
                    max_drawdown=self.max_drawdown
                )
            except Exception as e:
                print(f"Failed to save daily summary to database: {e}")
        
        # Export CSV report
        try:
            self._export_csv_report()
        except Exception as e:
            print(f"Failed to export CSV report: {e}")

    def _export_csv_report(self):
        """
        Export end-of-day report to CSV format.
        Creates two CSV files:
        1. Summary report (daily statistics)
        2. Detailed trades report (all trades for the day)
        """
        today = datetime.now().date()
        date_str = today.strftime("%Y%m%d")
        
        # Create reports directory if it doesn't exist
        reports_dir = "reports"
        os.makedirs(reports_dir, exist_ok=True)
        
        # 1. Export Summary Report
        summary_filename = os.path.join(reports_dir, f"eod_summary_{date_str}.csv")
        
        # Calculate metrics for summary
        win_loss_ratio = (
            round(self.wins / self.losses, 2)
            if self.losses > 0 else ("∞" if self.wins > 0 else 0.0)
        )
        win_rate = (
            round((self.wins / self.total_trades) * 100, 2)
            if self.total_trades > 0 else 0.0
        )
        avg_win = round(sum(self.win_pnls) / len(self.win_pnls), 2) if self.win_pnls else 0.0
        avg_loss = round(sum(self.loss_pnls) / len(self.loss_pnls), 2) if self.loss_pnls else 0.0
        total_wins = sum(self.win_pnls)
        total_losses = sum(self.loss_pnls)
        profit_factor = (
            round(total_wins / total_losses, 2)
            if total_losses > 0 else ("∞" if total_wins > 0 else 0.0)
        )
        max_win = round(max(self.win_pnls), 2) if self.win_pnls else 0.0
        max_loss = round(max(self.loss_pnls), 2) if self.loss_pnls else 0.0
        
        with open(summary_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow(["End of Day Report - Summary"])
            writer.writerow(["Date", today.strftime("%d %b %Y")])
            writer.writerow([])  # Empty row
            
            # Get total trade records for CSV
            total_trade_records = 0
            if self.trade_repo:
                try:
                    all_trades = self.trade_repo.get_date_trades(today)
                    total_trade_records = len(all_trades) if all_trades else 0
                except:
                    pass
            
            # Write metrics
            writer.writerow(["Metric", "Value"])
            writer.writerow(["Completed Trades (Closed Positions)", self.total_trades])
            if total_trade_records > 0:
                writer.writerow(["Total Trade Records (All Fills)", total_trade_records])
            writer.writerow(["Wins", self.wins])
            writer.writerow(["Losses", self.losses])
            writer.writerow(["Win Rate (%)", win_rate])
            writer.writerow(["Win/Loss Ratio", win_loss_ratio])
            writer.writerow(["Net PnL (₹)", round(self.net_pnl, 2)])
            writer.writerow(["Average Win (₹)", avg_win])
            writer.writerow(["Average Loss (₹)", avg_loss])
            writer.writerow(["Profit Factor", profit_factor])
            writer.writerow(["Max Win (₹)", max_win])
            writer.writerow(["Max Loss (₹)", max_loss])
            writer.writerow(["Max Drawdown (₹)", round(self.max_drawdown, 2)])
        
        print(f"Summary report exported to: {summary_filename}")
        
        # 2. Export Detailed Trades Report (if trade_repo is available)
        if self.trade_repo:
            try:
                trades = self.trade_repo.get_date_trades(today)
                if trades:
                    trades_filename = os.path.join(reports_dir, f"eod_trades_{date_str}.csv")
                    
                    with open(trades_filename, 'w', newline='', encoding='utf-8') as csvfile:
                        writer = csv.writer(csvfile)
                        
                        # Write header - includes trade_type to distinguish ENTRY vs EXIT
                        writer.writerow(["Trade ID", "Order ID", "Trade Type", "Entry Price", "Exit Price", 
                                       "Fill Price", "Quantity", "PnL (₹)", "Reason", "Timestamp"])
                        
                        # Write trade data
                        for trade in trades:
                            trade_type = trade.get("trade_type", "N/A")
                            timestamp = trade.get("timestamp")
                            timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S") if timestamp else "N/A"
                            
                            # Handle ENTRY vs EXIT trades differently
                            if trade_type == "ENTRY":
                                writer.writerow([
                                    trade.get("trade_id", "N/A"),
                                    trade.get("order_id", "N/A"),
                                    trade_type,
                                    trade.get("entry_price", trade.get("price", 0.0)),  # Entry price
                                    "",  # No exit price for ENTRY
                                    trade.get("price", 0.0),  # Fill price
                                    trade.get("quantity", 0),
                                    "",  # No PnL for ENTRY
                                    "",  # No reason for ENTRY
                                    timestamp_str
                                ])
                            elif trade_type == "EXIT":
                                writer.writerow([
                                    trade.get("trade_id", "N/A"),
                                    trade.get("order_id", "N/A"),
                                    trade_type,
                                    trade.get("entry_price", 0.0),
                                    trade.get("exit_price", trade.get("price", 0.0)),  # Exit price
                                    trade.get("price", 0.0),  # Fill price
                                    trade.get("quantity", 0),
                                    trade.get("pnl", 0.0),
                                    trade.get("reason", "N/A"),
                                    timestamp_str
                                ])
                            else:
                                # Fallback for old format or unknown type
                                writer.writerow([
                                    trade.get("trade_id", "N/A"),
                                    trade.get("order_id", "N/A"),
                                    trade_type,
                                    trade.get("entry_price", 0.0),
                                    trade.get("exit_price", 0.0),
                                    trade.get("price", 0.0),
                                    trade.get("quantity", 0),
                                    trade.get("pnl", 0.0),
                                    trade.get("reason", "N/A"),
                                    timestamp_str
                                ])
                    
                    print(f"Detailed trades report exported to: {trades_filename}")

                    # 3. Export Analyzer-style Trades CSV (round-trip format)
                    try:
                        analyzer_filename = os.path.join(reports_dir, f"analyzer_trades_{date_str}.csv")
                        self._export_analyzer_trades_csv(trades, analyzer_filename)
                        print(f"Analyzer trades report exported to: {analyzer_filename}")
                    except Exception as e:
                        print(f"Failed to export analyzer trades CSV: {e}")
                else:
                    print("No trades found for today - skipping detailed trades export")
            except Exception as e:
                print(f"Failed to export detailed trades: {e}")
                import traceback
                traceback.print_exc()
        else:
            print("Trade repository not available - skipping detailed trades export")

    def _export_analyzer_trades_csv(self, trades: list[dict], filename: str):
        """
        Export closed trades in the analyzer-compatible CSV format:
        entry_datetime, exit_datetime, symbol, side, quantity, entry_price, exit_price, reason

        Notes:
        - We use EXIT trade documents as "one completed trade".
        - For brokers using separate SL/TP order_ids, EXIT trades include entry_order_id + entry_datetime.
        """
        def _fmt_dt(dt_val):
            if not dt_val:
                return ""
            try:
                # dt_val is datetime
                return dt_val.strftime("%d-%m-%Y %H:%M")
            except Exception:
                try:
                    return str(dt_val)
                except Exception:
                    return ""

        def _map_reason(r):
            if not r:
                return ""
            r = str(r).upper()
            if r == "SL":
                return "STOPLOSS"
            if r == "TP":
                return "TAKEPROFIT"
            return r

        exit_trades = [t for t in (trades or []) if t.get("trade_type") == "EXIT"]

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                ["entry_datetime", "exit_datetime", "symbol", "side", "quantity", "entry_price", "exit_price", "reason"]
            )

            for t in exit_trades:
                entry_dt = t.get("entry_datetime")  # may be absent for older data
                exit_dt = t.get("timestamp")

                symbol = t.get("symbol") or ""
                side = "BUY"  # current strategy is long-only (BUY options)

                qty = t.get("quantity", 0) or 0
                entry_px = t.get("entry_price", "") or ""
                exit_px = t.get("exit_price", t.get("price", "")) or ""
                reason = _map_reason(t.get("reason"))

                writer.writerow(
                    [
                        _fmt_dt(entry_dt),
                        _fmt_dt(exit_dt),
                        symbol,
                        side,
                        qty,
                        entry_px,
                        exit_px,
                        reason,
                    ]
                )
