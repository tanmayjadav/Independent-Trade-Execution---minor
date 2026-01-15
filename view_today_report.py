#!/usr/bin/env python3
"""
Utility script to view trading performance report for a specific date.
Shows PnL, win/loss ratios, and other key metrics.
Automatically exports CSV reports (summary, trades, orders, and positions) to reports/ directory.

Usage:
    python view_today_report.py                    # View today's report + export CSV
    python view_today_report.py --date 2026-01-12  # View report for specific date + export CSV
    python view_today_report.py --date 2026-01-12 --json  # Export as JSON + CSV
"""

import json
import sys
import os
import csv
import argparse
from datetime import datetime, date
from database.trade_repo import TradeRepository


def load_config():
    """Load config from JSON file"""
    with open("config.json", "r") as f:
        config = json.load(f)
    return config, {}


def format_report(stats, report_date=None):
    """Format the statistics into a readable report."""
    
    # Determine if this is today's report
    is_today = report_date is None or report_date == datetime.now().date()
    
    print(f"\nDate: {stats['date']}")
    if not is_today:
        print(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    print("TRADE STATISTICS:")
    print()
    print(f"Total Trades: {stats['total_trades']}")
    print(f"Wins: {stats['wins']}")
    print(f"Losses: {stats['losses']}")
    print(f"Win Rate: {stats['win_rate']}%")
    print(f"Win/Loss Ratio: {stats['win_loss_ratio']}")
    print()
    
    print("PNL SUMMARY:")
    print()
    print(f"Net PnL: ₹{stats['net_pnl']:,.2f}")
    print(f"Average Win: ₹{stats['avg_win']:,.2f}")
    print(f"Average Loss: ₹{stats['avg_loss']:,.2f}")
    print(f"Max Win: ₹{stats['max_win']:,.2f}")
    print(f"Max Loss: ₹{stats['max_loss']:,.2f}")
    print(f"Profit Factor: {stats['profit_factor']}")
    print()
    
    print(f"Max Drawdown: ₹{stats['max_drawdown']:,.2f}")
    print()


def parse_date(date_string):
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_string, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(f"Invalid date format: {date_string}. Expected YYYY-MM-DD (e.g., 2026-01-12)")


def export_csv_reports(trade_repo, report_date, stats):
    """Export CSV reports (summary and detailed trades) for the specified date."""
    try:
        # Create reports directory if it doesn't exist
        reports_dir = "reports"
        os.makedirs(reports_dir, exist_ok=True)
        
        # Format date string for filename
        date_str = report_date.strftime("%Y%m%d")
        
        # 1. Export Summary Report
        summary_filename = os.path.join(reports_dir, f"eod_summary_{date_str}.csv")
        
        with open(summary_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow(["Metric", "Value"])
            
            # Write summary data
            writer.writerow(["Date", stats['date']])
            writer.writerow(["Total Trades", stats['total_trades']])
            writer.writerow(["Wins", stats['wins']])
            writer.writerow(["Losses", stats['losses']])
            writer.writerow(["Win Rate (%)", f"{stats['win_rate']:.2f}"])
            writer.writerow(["Win/Loss Ratio", f"{stats['win_loss_ratio']:.2f}"])
            writer.writerow(["Net PnL (₹)", f"{stats['net_pnl']:,.2f}"])
            writer.writerow(["Average Win (₹)", f"{stats['avg_win']:,.2f}"])
            writer.writerow(["Average Loss (₹)", f"{stats['avg_loss']:,.2f}"])
            writer.writerow(["Profit Factor", f"{stats['profit_factor']:.2f}"])
            writer.writerow(["Max Win (₹)", f"{stats['max_win']:,.2f}"])
            writer.writerow(["Max Loss (₹)", f"{stats['max_loss']:,.2f}"])
            writer.writerow(["Max Drawdown (₹)", f"{stats['max_drawdown']:,.2f}"])
        
        print(f"✓ Summary CSV exported: {summary_filename}")
        
        # 2. Export Detailed Trades Report
        trades = trade_repo.get_date_trades(report_date)
        if trades:
            trades_filename = os.path.join(reports_dir, f"eod_trades_{date_str}.csv")
            
            with open(trades_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write header
                writer.writerow([
                    "Trade ID", "Order ID", "Trade Type", "Price", 
                    "Quantity", "PnL (₹)", "Reason", "Timestamp"
                ])
                
                # Write trade data
                for trade in trades:
                    trade_type = trade.get("trade_type", "EXIT")  # Default to EXIT for backward compatibility
                    price = trade.get("price") or trade.get("entry_price") or trade.get("exit_price", 0.0)
                    
                    # For ENTRY trades, show entry price; for EXIT trades, show exit price
                    if trade_type == "ENTRY":
                        price_display = price
                        pnl_display = ""  # No PnL for entry trades
                        reason_display = ""  # No reason for entry trades
                    else:
                        price_display = price
                        pnl_display = f"{trade.get('pnl', 0.0):,.2f}"
                        reason_display = trade.get("reason", "")
                    
                    writer.writerow([
                        trade.get("trade_id", ""),
                        trade.get("order_id", ""),
                        trade_type,
                        f"{price_display:.2f}" if price_display else "",
                        trade.get("quantity", 0),
                        pnl_display,
                        reason_display,
                        trade.get("timestamp", "").strftime("%Y-%m-%d %H:%M:%S") if trade.get("timestamp") else ""
                    ])
            
            print(f"✓ Trades CSV exported: {trades_filename}")
        else:
            print(f"⚠ No trades found for {report_date}, skipping trades CSV export")
        
        # 3. Export Orders Report
        orders = trade_repo.get_date_orders(report_date)
        if orders:
            orders_filename = os.path.join(reports_dir, f"eod_orders_{date_str}.csv")
            
            with open(orders_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write header
                writer.writerow([
                    "Order ID", "Symbol", "Side", "Quantity", 
                    "Price", "Status", "Exchange", "Token", "Signal", "Timestamp"
                ])
                
                # Write order data
                for order in orders:
                    writer.writerow([
                        order.get("order_id", ""),
                        order.get("symbol", ""),
                        order.get("side", ""),
                        order.get("quantity", 0),
                        f"{order.get('price', 0.0):.2f}" if order.get("price") else "",
                        order.get("status", ""),
                        order.get("exchange", ""),
                        order.get("token", ""),
                        order.get("signal", ""),
                        order.get("timestamp", "").strftime("%Y-%m-%d %H:%M:%S") if order.get("timestamp") else ""
                    ])
            
            print(f"✓ Orders CSV exported: {orders_filename}")
        else:
            print(f"⚠ No orders found for {report_date}, skipping orders CSV export")
        
        # 4. Export Positions Report
        positions = trade_repo.get_date_positions(report_date)
        if positions:
            positions_filename = os.path.join(reports_dir, f"eod_positions_{date_str}.csv")
            
            with open(positions_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write header
                writer.writerow([
                    "Symbol", "Entry Price", "Exit Price", "Quantity", 
                    "Status", "Order IDs", "Updated At"
                ])
                
                # Write position data
                for position in positions:
                    order_ids = position.get("order_ids", [])
                    order_ids_str = ", ".join(order_ids) if isinstance(order_ids, list) else str(order_ids)
                    
                    writer.writerow([
                        position.get("symbol", ""),
                        f"{position.get('entry_price', 0.0):.2f}" if position.get("entry_price") else "",
                        f"{position.get('exit_price', 0.0):.2f}" if position.get("exit_price") else "",
                        position.get("quantity", 0),
                        position.get("status", ""),
                        order_ids_str,
                        position.get("updated_at", "").strftime("%Y-%m-%d %H:%M:%S") if position.get("updated_at") else ""
                    ])
            
            print(f"✓ Positions CSV exported: {positions_filename}")
        else:
            print(f"⚠ No positions found for {report_date}, skipping positions CSV export")
            
    except Exception as e:
        print(f"⚠ Error exporting CSV reports: {e}", file=sys.stderr)


def main():
    """Main function to generate and display trading report for a specific date."""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="View trading performance report for a specific date",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python view_today_report.py                    # View today's report + export CSV
  python view_today_report.py --date 2026-01-12  # View report for Jan 12, 2026 + export CSV
  python view_today_report.py -d 2026-01-12 --json  # Export as JSON + CSV
  
Note: CSV files are automatically exported to reports/ directory:
  - eod_summary_YYYYMMDD.csv (summary statistics)
  - eod_trades_YYYYMMDD.csv (detailed trades list)
  - eod_orders_YYYYMMDD.csv (all orders for the date)
  - eod_positions_YYYYMMDD.csv (all positions updated on the date)
        """
    )
    parser.add_argument(
        "--date", "-d",
        type=str,
        help="Date in YYYY-MM-DD format (default: today)"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Export report as JSON"
    )
    
    args = parser.parse_args()
    
    try:
        # Parse date argument
        if args.date:
            report_date = parse_date(args.date)
        else:
            report_date = datetime.now().date()
        
        # Load configuration
        config, _ = load_config()
        
        # Get database connection details
        mongo_uri = config["deployment"].get("mongo_uri", "mongodb://localhost:27017")
        db_name = config["deployment"].get("db_name", "ema_xts")
        
        # Initialize repository
        trade_repo = TradeRepository(mongo_uri=mongo_uri, db_name=db_name)
        
        # Get statistics for the specified date
        stats = trade_repo.get_date_stats(report_date)
        
        # Display report
        if not args.json:
            format_report(stats, report_date)
            # Export CSV reports automatically
            print("\n" + "="*50)
            print("EXPORTING CSV REPORTS...")
            print("="*50)
            export_csv_reports(trade_repo, report_date, stats)
        else:
            # Export as JSON
            print(json.dumps(stats, indent=2))
            # Also export CSV when using --json flag
            export_csv_reports(trade_repo, report_date, stats)
        
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error generating report: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

