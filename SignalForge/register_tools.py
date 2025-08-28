from tools.get_ticker_data import get_ticker_data
from tools.get_options_data import get_options_data
from tools.analyze_technical_indicators import analyze_technical_indicators
from tools.analyze_fundamentals import analyze_fundamentals
from tools.summarize_insights import summarize_insights

def get_tool_definitions():
    return {
        "get_ticker_data": get_ticker_data,
        "get_options_data": get_options_data,
        "analyze_technical_indicators": analyze_technical_indicators,
        "analyze_fundamentals": analyze_fundamentals,
        "summarize_insights": summarize_insights
    }
