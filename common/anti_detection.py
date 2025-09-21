# common/anti_detection.py

def get_stealth_launch_args() -> dict:
    """
    Returns a dictionary of launch arguments to make Playwright harder to detect.
    """
    return {
        "args": [
            '--disable-blink-features=AutomationControlled',
        ]
    }

def get_stealth_context_options(user_agent: str = None) -> dict:
    """
    Returns a dictionary of options for a new browser context.
    """
    user_agent = user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    
    return {
        "user_agent": user_agent,
        # Playwright is generally stealthier, but you can add more options here
        # For example, setting timezone, locale, color scheme, etc.
        # "timezone_id": "Asia/Kolkata",
        # "locale": "en-IN",
    }