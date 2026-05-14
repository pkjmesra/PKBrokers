#!/usr/bin/env python3
"""
Filter and categorize KiteTokenWatcher logs for debugging.
Usage: python filter_kite_logs.py <input_log_file> [output_dir]
"""

import re
import sys
import os
from collections import defaultdict
from datetime import datetime

# Keywords to track (case-insensitive)
KEYWORDS = {
    'ERRORS': ['ERROR', 'EXCEPTION', 'FAILED', 'CRITICAL'],
    'WARNINGS': ['WARNING', 'stale', 'health', 'recovery'],
    'WEBSOCKET': ['websocket', 'websocket_index', 'connection', 'disconnect', 'reconnect'],
    'PROCESSING': ['process_tick', 'batch', 'queue', 'processing', '_process_tick_batch'],
    'HEALTH_MONITOR': ['TickHealthMonitor', 'health_monitor', '_trigger_recovery', 'recovering'],
    'STATS': ['instruments', 'ticks_processed', 'unique instruments', 'queued'],
    'TIMING': ['elapsed', 'timeout', 'delay', 'threshold']
}

def parse_timestamp(line):
    """Extract timestamp from log line."""
    # Pattern for [2026-05-13 03:44:09]
    match = re.search(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]', line)
    if match:
        return match.group(1)
    return None

def categorize_line(line):
    """Categorize a log line based on keywords."""
    line_lower = line.lower()
    categories = []
    
    for category, keywords in KEYWORDS.items():
        for keyword in keywords:
            if keyword.lower() in line_lower:
                categories.append(category)
                break
    
    return categories if categories else ['OTHER']

def filter_logs(input_file, output_dir=None):
    """Filter and categorize logs from input file."""
    
    if not os.path.exists(input_file):
        print(f"Error: File {input_file} not found")
        return
    
    if output_dir is None:
        output_dir = os.path.dirname(input_file) or '.'
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Open output files
    output_files = {}
    for category in list(KEYWORDS.keys()) + ['OTHER', 'ALL']:
        output_files[category] = open(os.path.join(output_dir, f'kite_logs_{category}.txt'), 'w', encoding='utf-8')
    
    # Also open a combined file
    combined_file = open(os.path.join(output_dir, 'kite_logs_ALL.txt'), 'w', encoding='utf-8')
    
    stats = defaultdict(int)
    time_events = []
    
    print(f"Processing {input_file}...")
    
    with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            # Write to combined file
            combined_file.write(line)
            
            # Categorize and write to specific files
            categories = categorize_line(line)
            for category in categories:
                output_files[category].write(line)
                stats[category] += 1
            
            # Also write to OTHER if no category matched
            if not categories:
                output_files['OTHER'].write(line)
                stats['OTHER'] += 1
            
            # Extract timestamp for time-series analysis
            ts = parse_timestamp(line)
            if ts and any(c in categories for c in ['ERRORS', 'WARNINGS', 'HEALTH_MONITOR']):
                time_events.append((ts, line.strip()[:100]))
    
    # Close all files
    for f in output_files.values():
        f.close()
    combined_file.close()
    
    # Print summary
    print("\n" + "="*60)
    print("FILTERING COMPLETE")
    print("="*60)
    print(f"Output directory: {output_dir}")
    print("\nFile breakdown:")
    for category, count in sorted(stats.items()):
        print(f"  kite_logs_{category}.txt: {count} lines")
    
    print(f"\nTotal lines processed: {sum(stats.values())}")
    
    # Print key events timeline
    print("\n" + "="*60)
    print("KEY EVENT TIMELINE (Errors/Warnings/Health Events)")
    print("="*60)
    for ts, event in time_events[:50]:  # First 50 events
        print(f"{ts} | {event}")
    if len(time_events) > 50:
        print(f"... and {len(time_events) - 50} more events")

def extract_recovery_logs(input_file):
    """Specifically extract recovery-related logs."""
    recovery_patterns = [
        r'_trigger_recovery',
        r'recovering',
        r'TickHealthMonitor',
        r'health.*warning',
        r'stale.*instruments',
        r'recovery',
        r'reconnect',
        r'restarting',
        r'WebSocket.*stop',
        r'WebSocket.*start',
    ]
    
    pattern = re.compile('|'.join(recovery_patterns), re.IGNORECASE)
    
    output_file = input_file + '.recovery_only.txt'
    
    with open(input_file, 'r', encoding='utf-8', errors='ignore') as f_in:
        with open(output_file, 'w', encoding='utf-8') as f_out:
            for line in f_in:
                if pattern.search(line):
                    f_out.write(line)
    
    print(f"\nRecovery-only logs saved to: {output_file}")
    return output_file

def generate_summary_report(input_file):
    """Generate a summary report of key issues."""
    
    with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    report_lines = []
    report_lines.append("="*60)
    report_lines.append("KITETOKENWATCHER ISSUES SUMMARY")
    report_lines.append("="*60)
    
    # Check for recovery triggers
    recovery_triggers = re.findall(r'_trigger_recovery', content, re.IGNORECASE)
    report_lines.append(f"\n1. Recovery Triggers: {len(recovery_triggers)} found")
    if len(recovery_triggers) == 0:
        report_lines.append("   ⚠️ WARNING: No recovery triggers found! Health monitor is detecting issues but not acting.")
    
    # Check for stale warnings
    stale_warnings = re.findall(r'TICK HEALTH WARNING.*?(\d+)/(\d+).*?for (\d+) seconds', content)
    if stale_warnings:
        last_stale = stale_warnings[-1]
        report_lines.append(f"\n2. Last Stale Warning: {last_stale[0]}/{last_stale[1]} instruments stale for {last_stale[2]}s")
        report_lines.append(f"   Percentage: {int(last_stale[0])/int(last_stale[1])*100:.1f}%")
    
    # Check for WebSocket issues
    ws_issues = re.findall(r'Websocket_index:\d+.*?Running Count.*?:(\d+)', content)
    if ws_issues:
        ws_counts = [int(c) for c in ws_issues if c.isdigit()]
        if ws_counts:
            report_lines.append(f"\n3. WebSocket Distribution: min={min(ws_counts)}, max={max(ws_counts)}, ratio={max(ws_counts)/max(min(ws_counts),1):.1f}x")
            if max(ws_counts) / max(min(ws_counts), 1) > 10:
                report_lines.append("   ⚠️ UNEVEN DISTRIBUTION - Some WebSocket connections are not receiving ticks")
    
    # Check for empty JSON writes
    empty_json = re.findall(r'Saved empty ticks\.json', content)
    report_lines.append(f"\n4. Empty ticks.json saves: {len(empty_json)} times")
    if len(empty_json) > 0:
        report_lines.append("   ⚠️ CRITICAL: No tick data reaching JSON writer!")
    
    # Check for batch processing
    batches = re.findall(r'Processing batch with (\d+) unique instruments', content)
    if batches:
        last_batch = batches[-1]
        report_lines.append(f"\n5. Last batch size: {last_batch} instruments")
    
    # Check for queue issues
    queue_lines = re.findall(r'Queued (\d+) instruments for processing', content)
    if queue_lines:
        report_lines.append(f"\n6. Last queue size: {queue_lines[-1]} instruments")
    
    # Recommendations
    report_lines.append("\n" + "="*60)
    report_lines.append("RECOMMENDATIONS")
    report_lines.append("="*60)
    
    if len(recovery_triggers) == 0:
        report_lines.append("• Add _trigger_recovery() call in health monitor when stale threshold exceeded")
    
    if len(empty_json) > 0:
        report_lines.append("• Check JSON writer initialization - ticks are not reaching it")
        report_lines.append("• Verify _watcher_queue is receiving data from WebSocket client")
    
    if ws_counts and max(ws_counts) / max(min(ws_counts), 1) > 10:
        report_lines.append("• Rebalance token batches - some WebSocket processes may be overloaded")
        report_lines.append("• Consider reducing OPTIMAL_TOKEN_BATCH_SIZE")
    
    report_lines.append("\n• Share these filtered logs for further analysis:")
    report_lines.append("  - kite_logs_HEALTH_MONITOR.txt")
    report_lines.append("  - kite_logs_ERRORS.txt")
    report_lines.append("  - kite_logs_WEBSOCKET.txt")
    
    report = '\n'.join(report_lines)
    print(report)
    
    # Save report
    report_file = input_file + '.summary_report.txt'
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    return report_file

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python LogAnalyzer.py <log_file> [output_dir]")
        print("\nExample: python LogAnalyzer.py /path/to/0_market-runner_1.txt ./filtered_logs/")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Run filtering
    filter_logs(input_file, output_dir)
    
    # Extract recovery-specific logs
    if output_dir:
        recovery_file = extract_recovery_logs(os.path.join(output_dir, 'kite_logs_ALL.txt'))
    else:
        recovery_file = extract_recovery_logs(input_file + '.filtered')
    
    # Generate summary report
    if output_dir:
        report_file = generate_summary_report(os.path.join(output_dir, 'kite_logs_ALL.txt'))
    else:
        report_file = generate_summary_report(input_file)
    
    print(f"\n📊 Summary report saved to: {report_file}")
    print("\n📁 Files created:")
    print("   - kite_logs_ALL.txt (all filtered logs)")
    print("   - kite_logs_ERRORS.txt (errors only)")
    print("   - kite_logs_WARNINGS.txt (warnings only)")
    print("   - kite_logs_WEBSOCKET.txt (WebSocket related)")
    print("   - kite_logs_HEALTH_MONITOR.txt (health monitor specific)")
    print("   - kite_logs_PROCESSING.txt (tick processing logs)")
    print("   - kite_logs_STATS.txt (statistics)")
    print("   - kite_logs_TIMING.txt (timing related)")
    print("   - kite_logs_OTHER.txt (uncategorized)")
    print("   - {log_file}.recovery_only.txt (recovery events only)")
    print("   - {log_file}.summary_report.txt (issues summary)")