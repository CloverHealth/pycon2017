"""
Methods for fitting a beta binomial model, to be used for skills measurement and
A/B test analysis.
"""
import numpy as np
import scipy.stats as scs


def beta_values(alpha, beta, numpoints=1000):
    domain = np.linspace(0, 1, numpoints)
    dist = scs.beta.pdf(domain, alpha, beta)
    return (domain, dist,)


def fit_beta_prior(data):
    """
    Fit the parameters of a beta prior given probability data.
    Arguments:
        data (numpy array):
            Values ranging 0 to 1, representing the population level data.

    Returns:
        List of beta distribution parameters
    """
    alpha, beta, _, _ = scs.beta.fit(data, floc=0, fscale=1)
    return (alpha, beta,)


def beta_posterior_values(successes, trials, alpha, beta, numpoints=1000):
    failures = trials - successes
    return beta_values(alpha + successes, beta + failures, numpoints)


def beta_expected_value(alpha, beta):
    return alpha / (alpha + beta)


def compare_beta_distros(alpha_sa, beta_sa, alpha_sb, beta_sb):
    """
    Evaluate the probability that a random sample of the beta distribution from
    sample a is greater than a random sample from the beta distribution of sample b.
    This gives a good measure of whether sample a is *better* than sample b.
    """
    sa_samples = np.random.beta(alpha_sa, beta_sa, 100000)
    sb_samples = np.random.beta(alpha_sb, beta_sb, 100000)
    return np.sum(sa_samples > sb_samples) / len(sa_samples)
