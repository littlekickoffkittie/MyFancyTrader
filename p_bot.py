try:
    from .bots.live_bot import *
except (ImportError, ValueError):
    try:
        from bots.live_bot import *
    except ImportError:
        try:
            from fangblenny_bot.bots.live_bot import *
        except ImportError:
            # If all else fails, try to import from parent directory
            import sys
            import os
            sys.path.append(os.path.join(os.path.dirname(__file__), 'bots'))
            from live_bot import *
