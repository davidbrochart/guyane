import math as math

def sh1(t, x4):
    if t == 0:
        res = 0.
    elif t < x4:
        res = (float(t) / x4) ** (5. / 2.)
    else:
        res = 1.
    return res

def sh2(t, x4):
    if t == 0:
        res = 0.
    elif t < x4:
        res = 0.5 * ((float(t) / x4) ** (5. / 2.))
    elif t < 2. * x4:
        res = 1. - 0.5 * ((2. - float(t) / x4) ** (5. / 2.))
    else:
        res = 1.
    return res

def uh1(j, x4):
    return sh1(j, x4) - sh1(j - 1, x4)

def uh2(j, x4):
    return sh2(j, x4) - sh2(j - 1, x4)

def gr4j(_x, _obs, _state = None):
    x1, x2, x3, x4 = _x
    if _state == None:
        s = x1 / 2.
        r = x3 / 2.
        pr_prev = [0.] * int(2. * x4)
    else:
        s = _state[0]
        r = _state[1]
        pr_prev = _state[2:]
    _p, _e = _obs
    l = int(x4) + 1
    m = int(2. * x4) + 1
    # reservoir de production:
    if _p > _e:
        pn = _p - _e
        en = 0.
        ps = x1 * (1. - (s / x1) ** 2) * math . tanh(pn / x1) / ( 1. + (s / x1) * math . tanh(pn / x1))
        s += ps
    elif _p < _e:
        ps = 0.
        pn = 0.
        en = _e - _p
        es = s * (2. - s / x1) * math . tanh(en / x1) / (1. + (1. - s / x1) * math . tanh(en / x1))
        s = max(0., s - es)
    else:
        pn = 0.
        en = 0.
        ps = 0.
    # percolation:
    perc = s * (1. - (1. + ((4. * s / (9. * x1)) ** 4)) ** (-1. / 4.))
    s -= perc
    # hydrogrammes:
    pr = [None] * m
    for i in range(len(pr_prev)):
        pr[i + 1] = pr_prev[i]
    pr[0] = perc + pn - ps
    q9 = 0.
    q1 = 0.
    for k in range(l):
        q9 += uh1(k + 1, x4) * pr[k]
    for k in range(m):
        q1 += uh2(k + 1, x4) * pr[k]
    q9 *= 0.9
    q1 *= 0.1
    # echange souterrain:
    f = x2 * ((r / x3) ** (7. / 2.))
    # reservoir de routage:
    r = max(0., r + q9 + f)
    qr = r * (1. - ((1. + (r / x3) ** 4) ** (-1. / 4.)))
    r -= qr
    if r > x3:
        s = x3
        qr += s - x3
    qd = max(0., q1 + f)
    q = qr + qd
    return q, [s, r] + pr[:-1]
