from scipy.stats import norm
import math

def BSCall(s: float, k: float, r: float, q: float, vol: float, t: float, greek_type='Price'):
    d1 = (math.log(s / k) + (r - q + (vol ** 2) / 2) * t) / (vol * math.sqrt(t))
    d2 = (math.log(s / k) + (r - q - (vol ** 2) / 2) * t) / (vol * math.sqrt(t))

    if greek_type=='Price':
        return math.exp(-q * t) * s * norm.cdf(d1) - math.exp(-r * t) * k * norm.cdf(d2)
    else: #Assuming greek_type is 'Delta'
        return math.exp(-q * t) * norm.cdf(d1)
        

def BSPut(s: float, k: float, r: float, q: float, vol: float, t: float, greek_type='Price'):
    d1 = (math.log(s / k) + (r - q + (vol ** 2) / 2) * t) / (vol * math.sqrt(t))
    d2 = (math.log(s / k) + (r - q - (vol ** 2) / 2) * t) / (vol * math.sqrt(t))

    if greek_type=='Price':
        return k * math.exp(-r * t) * norm.cdf(-d2) - math.exp(-q * t) * s * norm.cdf(-d1)
    else: #Assuming greek_type is 'Delta'
        return -1 * math.exp(-q * t) * (norm.cdf(-d1))
    
def BSDigitalCall(s: float, k: float, r: float, q: float, vol: float, t: float, Payoff: float, greek_type='Price'):
    """
    https://quantpie.co.uk/bsm_bin_c_formula/bs_bin_c_summary.php
    """
    d1 = (math.log(s / k) + (r - q + (vol ** 2) / 2) * t) / (vol * math.sqrt(t))
    d2 = d1 - vol * math.sqrt(t)

    if greek_type=='Price':
        return Payoff * math.exp(-r * t) * norm.cdf(d2) 
    else: #Assuming greek_type is 'Delta'
        return math.exp(-r * t) * norm.pdf(d2) / (vol * s * math.sqrt(t))
    
def BSDigitalPut(s: float, k: float, r: float, q: float, vol: float, t: float, Payoff: float, greek_type='Price'):
    d1 = (math.log(s / k) + (r - q + (vol ** 2) / 2) * t) / (vol * math.sqrt(t))
    d2 = d1 - vol * math.sqrt(t)

    if greek_type=='Price':
        return Payoff * math.exp(-r * t) * norm.cdf(-d2)
    else: #Assuming greek_type is 'Delta'
        return -math.exp(-r * t) * norm.pdf(d2) / (vol * s * math.sqrt(t))