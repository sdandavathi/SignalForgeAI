from register_tools import get_tool_definitions

def run_workflow(ticker: str):
    tools = get_tool_definitions()
    outputs = {}
    outputs["get_ticker_data"] = tools["get_ticker_data"](ticker)
    outputs["get_options_data"] = tools["get_options_data"](ticker)
    outputs["analyze_technical_indicators"] = tools["analyze_technical_indicators"](ticker)
    outputs["analyze_fundamentals"] = tools["analyze_fundamentals"](ticker)
    summary = tools["summarize_insights"](outputs)
    return summary

if __name__ == "__main__":
    print(run_workflow("AAPL"))
