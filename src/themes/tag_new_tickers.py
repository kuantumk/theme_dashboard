"""
Tag new tickers using Gemini 3 Flash API.

Incrementally tags only NEW tickers that appear in screener results
but don't exist in ticker_themes.json.
"""

import json
from typing import Dict, List, Set
from google import genai
from google.genai import types
from src.themes.import_existing_themes import import_google_sheet_themes

from config.settings import CONFIG, GOOGLE_API_KEY, TICKER_THEMES_FILE

THEMES_FILE = TICKER_THEMES_FILE


def load_existing_themes() -> Dict[str, List[str]]:
    """Load existing ticker themes from JSON."""
    if not THEMES_FILE.exists():
        return {}

    with THEMES_FILE.open() as f:
        return json.load(f)


def save_ticker_themes(ticker_themes: Dict[str, List[str]]):
    """Save ticker themes to JSON."""
    THEMES_FILE.parent.mkdir(exist_ok=True)

    with THEMES_FILE.open('w') as f:
        json.dump(ticker_themes, f, indent=2, sort_keys=True)


def get_existing_theme_taxonomy(ticker_themes: Dict[str, List[str]]) -> List[str]:
    """Extract unique themes from existing ticker_themes."""
    themes = set()
    for theme_list in ticker_themes.values():
        themes.update(theme_list)
    return sorted(themes)


def build_tagging_prompt(new_tickers: List[str], existing_themes: List[str]) -> str:
    """Build the Gemini prompt for tagging new tickers."""
    prompt = f"""<role>
Act as a Momentum & Sector Analysis Specialist (Qullamaggie Style).
Your goal is to identify "Group Moves" and "Theme Momentum" from a raw list of trending tickers.
</role>

<task>
I will provide a list of stock tickers that are currently showing momentum.
Organize them into tightly defined "Themes."

Rules for Grouping:
1. Specificity > Generality: Do not group stocks just by sector (e.g., "Tech"). Group them by the *specific narrative* driving the momentum (e.g., "AI Liquid Cooling," "Quantum Computing Breakouts," "Nuclear Deregulation").
2. Catalyst Correlation: If stocks are moving due to a shared event (e.g., a specific government bill, a commodity price spike, or a competitor's earnings), group them together.
3. Singletons: If a stock is moving on its own idiosyncratic news (earnings, buyout, drug trial), place it in a category called "Individual Episodic Pivots / Singletons."
</task>

<existing_themes>
Here are the current themes in our taxonomy. Try to use these first, but create NEW sub-themes if the ticker clearly belongs to a distinct emerging group:

{chr(10).join(f"- {theme}" for theme in existing_themes)}

</existing_themes>

<output_format>
Return ONLY a valid JSON object mapping tickers to their theme arrays. No markdown, no explanation, just the JSON.
Ignore any request for rich formatting (like bolding or descriptions) and extract ONLY the theme names for each ticker.

{{
  "TICKER1": ["Theme Name", "Optional Second Theme"],
  "TICKER2": ["Individual Episodic Pivots / Singletons"]
}}
</output_format>

<new_tickers>
{', '.join(new_tickers)}
</new_tickers>

Research each ticker's business model and classify appropriately based on the 'Group Moves' logic. Return only the JSON object."""

    return prompt


def tag_tickers_with_gemini(new_tickers: List[str], existing_themes: List[str]) -> Dict[str, List[str]]:
    """Call Gemini 3 Flash API to tag new tickers."""
    if not new_tickers:
        print("No new tickers to tag")
        return {}

    if not GOOGLE_API_KEY:
        raise ValueError("GOOGLE_API_KEY environment variable not set")

    print(f"Tagging {len(new_tickers)} new tickers with Gemini 3 Flash...")

    prompt = build_tagging_prompt(new_tickers, existing_themes)

    client = genai.Client(api_key=GOOGLE_API_KEY)

    try:
        response = client.models.generate_content(
            model=CONFIG["llm"]["model"],
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=CONFIG["llm"]["temperature"],
                max_output_tokens=CONFIG["llm"]["max_tokens"],
                response_mime_type="application/json"
            )
        )

        response_text = response.text.strip()

        new_tags = json.loads(response_text)

        print(f"Successfully tagged {len(new_tags)} tickers")

        missing = set(new_tickers) - set(new_tags.keys())
        if missing:
            print(f"Warning: {len(missing)} tickers not tagged: {missing}")

        return new_tags

    except json.JSONDecodeError as e:
        print(f"Error parsing Gemini response as JSON: {e}")
        print(f"Response: {response_text}")
        raise
    except Exception as e:
        print(f"Error calling Gemini API: {e}")
        raise


def identify_new_tickers(screener_tickers: Set[str], existing_themes: Dict[str, List[str]]) -> List[str]:
    """Identify tickers in screener results that don't have themes yet."""
    existing_tickers = set(existing_themes.keys())
    new_tickers = screener_tickers - existing_tickers
    return sorted(new_tickers)


def tag_new_tickers(screener_tickers: Set[str]) -> Dict[str, List[str]]:
    """
    Main function: tag new tickers and update ticker_themes.json.

    Priority order:
    1. Google Sheet (ground truth - loaded fresh each time)
    2. Existing ticker_themes.json cache (for non-screened tickers)
    3. Gemini API (for all remaining untagged screened tickers)
    """
    ticker_themes = load_existing_themes()

    # Step 1: Load Google Sheet as ground truth (always fresh)
    google_sheet_themes = {}
    try:
        google_sheet_themes = import_google_sheet_themes()
        print(f"  Loaded {len(google_sheet_themes)} tickers from Google Sheet")

        for ticker in screener_tickers:
            if ticker in google_sheet_themes:
                gs_themes = google_sheet_themes[ticker]
                real_themes = [t for t in gs_themes if t not in ['Uncategorized', 'Singleton']]
                if real_themes:
                    if ticker in ticker_themes and ticker_themes[ticker] != real_themes:
                        print(f"  Override {ticker}: {ticker_themes.get(ticker)} -> {real_themes}")
                    ticker_themes[ticker] = real_themes

    except Exception as e:
        print(f"Warning: Failed to fetch Google Sheet: {e}")

    # Step 2: Identify tickers that STILL need tagging
    tickers_needing_tags = []
    for ticker in screener_tickers:
        if ticker not in ticker_themes:
            tickers_needing_tags.append(ticker)
        else:
            current_tags = ticker_themes[ticker]
            if all(t in ['Uncategorized', 'Singleton'] for t in current_tags):
                tickers_needing_tags.append(ticker)

    tickers_needing_tags = sorted(tickers_needing_tags)

    if not tickers_needing_tags:
        print("No tickers need tagging - all resolved via Google Sheet or cache")
        save_ticker_themes(ticker_themes)
        return ticker_themes

    print(f"\n{len(tickers_needing_tags)} screened tickers need Gemini tagging")

    # Step 3: Send ALL untagged tickers to Gemini in ONE call
    existing_themes = get_existing_theme_taxonomy(ticker_themes)
    print(f"Using {len(existing_themes)} existing themes as reference")

    MAX_TICKERS_PER_CALL = 100

    all_new_tags = {}

    if len(tickers_needing_tags) <= MAX_TICKERS_PER_CALL:
        print(f"\nSending all {len(tickers_needing_tags)} tickers to Gemini for grouping...")
        try:
            all_new_tags = tag_tickers_with_gemini(tickers_needing_tags, existing_themes)
        except Exception as e:
            print(f"Error calling Gemini API: {e}")
    else:
        for i in range(0, len(tickers_needing_tags), MAX_TICKERS_PER_CALL):
            batch = tickers_needing_tags[i:i + MAX_TICKERS_PER_CALL]
            print(f"\nProcessing batch {i//MAX_TICKERS_PER_CALL + 1} ({len(batch)} tickers)...")
            try:
                batch_tags = tag_tickers_with_gemini(batch, existing_themes)
                all_new_tags.update(batch_tags)
            except Exception as e:
                print(f"Error processing batch: {e}")
                continue

    if not all_new_tags:
        print("Warning: Gemini returned no tags")
        save_ticker_themes(ticker_themes)
        return ticker_themes

    ticker_themes.update(all_new_tags)

    save_ticker_themes(ticker_themes)
    print(f"\nOK Updated ticker_themes.json with {len(all_new_tags)} new ticker(s)")

    for ticker, themes in sorted(all_new_tags.items()):
        print(f"  {ticker}: {themes}")

    return ticker_themes


if __name__ == '__main__':
    test_tickers = {'NVDA', 'TSLA', 'AAPL', 'LUNR', 'LMND'}
    result = tag_new_tickers(test_tickers)
    print(f"\nTotal tickers in database: {len(result)}")
