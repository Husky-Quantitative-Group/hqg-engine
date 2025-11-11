from typing import List, Tuple, Dict, Optional

def aggregate_allocations(allocations: List[Tuple[str, List[Tuple[str, float]], float]]):
    """
    Take in a list of strategy allocations (strategy_id, allocations (list of (ticker, weight) tuples), aum_weight))
    
    Returns a dict of {ticker : weight} for the final portfolio allocation
    """
    
    final_weights = {}
    
    for strategy_id, allocations, aum_weight in allocations:
        total_weight = sum(weight for _, weight in allocations)
        
        if total_weight > 1:  # only validate that weights don't exceed 1.0
            raise ValueError(f"{strategy_id} allocations sum to {total_weight} (exceeds 1.0)")
        
        for ticker, weight in allocations:
            if ticker in final_weights:
                final_weights[ticker] += weight * aum_weight
            else:
                final_weights[ticker] = weight * aum_weight
    
    return final_weights