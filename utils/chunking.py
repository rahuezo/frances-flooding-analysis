def chunkify(l, N, smooth=False): 
    chunks = [l[i:i+N] for i in xrange(0, len(l), N)]
    if smooth and len(chunks) > 1 and len(chunks[-1]) != N: 
        chunks[-2].extend(chunks.pop())
    return chunks