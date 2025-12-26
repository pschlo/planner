from collections.abc import Sequence


def best_subsequence_window(context, seq):
    m, n = len(context), len(seq)
    if m == 0:
        return (0, -1)

    best = None
    i = 0
    while True:
        # forward scan to complete context
        ci = 0
        j = i
        while j < n and ci < m:
            if seq[j] == context[ci]:
                ci += 1
            j += 1
        if ci < m:
            break  # no completion from i

        end = j - 1

        # backward tighten
        ci = m - 1
        k = end
        start = None
        while k >= i:
            if seq[k] == context[ci]:
                ci -= 1
                if ci < 0:
                    start = k
                    break
            k -= 1

        if start is None:
            return best  # safety

        if best is None or (end - start) < (best[1] - best[0]):
            best = (start, end)

        i = start + 1
        if i >= n:
            break

    return best

def strict_order_match_score(context, edge_path_keys,
                             length_weight=1.0,
                             early_tie_breaker=0.0,
                             epsilon=1e-9) -> float:
    """
    Returns float in [0,1].
    - 0 if `context` is NOT a subsequence of `edge_path_keys`.
    - Otherwise: score = coverage^length_weight * compactness
      where coverage = (len(context)+epsilon)/(len(seq)+epsilon),
            compactness = 1/(1+gaps) using the SHORTEST window containing the subsequence.
    - If context is empty: returns a tiny positive ≈ epsilon/(n+epsilon).
    """
    m, n = len(context), len(edge_path_keys)

    # Empty context: subsequence vacuously true; tiny positive score
    if m == 0:
        coverage = (epsilon / (n + epsilon)) ** max(1.0, float(length_weight)) if n > 0 else 1.0
        return coverage  # compactness=1, no gaps

    win = best_subsequence_window(context, edge_path_keys)
    if win is None:
        return 0.0

    start, end = win
    span_len = end - start + 1
    gaps = span_len - m

    coverage = ((m + epsilon) / (n + epsilon)) ** max(1.0, float(length_weight))
    compactness = 1.0 / (1.0 + gaps)
    base = coverage * compactness

    if early_tie_breaker > 0.0:
        early = 1.0 / (1.0 + start)  # prefer earlier window slightly
        return (base + early_tie_breaker * early) / (1.0 + early_tie_breaker)
    return base



# score = strict_order_match_score(
#     list(),
#     list("ab")
# )
# print(score)
