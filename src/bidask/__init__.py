"""Bid/ask tape-pressure dashboard — a standalone local app.

Deliberately import-free. `feed` pulls in `tradingview_screener`, and an eager
import here would make that dependency reachable from anything that touches the
package, turning a vendor schema break into a failure for unrelated code.
Import the submodules you need directly.
"""
