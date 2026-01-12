"""Matching function registry and base functionality."""

from typing import Callable, List, Dict, Any

# Registry for matching functions
MATCHING_FUNCTIONS: List[Dict[str, Any]] = []


def register_matching_function(
    name: str,
    func: Callable,
    description: str = "",
    enabled: bool = True
):
    """
    Register a matching function to be executed during the matching process.
    
    Args:
        name: Unique name for the matching function
        func: Function that takes (duck, table_name) and performs matching
        description: Optional description of what the function does
        enabled: Whether this function is enabled (default: True)
    """
    MATCHING_FUNCTIONS.append({
        'name': name,
        'func': func,
        'description': description,
        'enabled': enabled
    })


def run_matching_functions(duck, table_name):
    """Execute all registered and enabled matching functions."""
    enabled_functions = [f for f in MATCHING_FUNCTIONS if f['enabled']]
    
    if not enabled_functions:
        print("⚠ No matching functions registered or enabled")
        return
    
    print(f"\n--- Running {len(enabled_functions)} matching function(s) ---")
    for i, match_func in enumerate(enabled_functions, 1):
        print(f"\n[{i}/{len(enabled_functions)}] {match_func['name']}")
        if match_func['description']:
            print(f"  Description: {match_func['description']}")
        try:
            match_func['func'](duck, table_name)
        except Exception as e:
            print(f"  ✗ Error in {match_func['name']}: {e}")
            raise


def setup_default_matching_functions():
    """Register the default matching functions."""
    from .name_matcher import match_by_name
    from .address_matcher import match_by_address
    
    register_matching_function(
        name="name_match",
        func=match_by_name,
        description="Match firms by name using cleaned names and Jaro-Winkler similarity"
    )
    
    register_matching_function(
        name="address_match",
        func=match_by_address,
        description="Match firms by address (road, postcode, house number)"
    )


__all__ = [
    'MATCHING_FUNCTIONS',
    'register_matching_function',
    'run_matching_functions',
    'setup_default_matching_functions'
]
