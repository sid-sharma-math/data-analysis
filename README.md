# data-analysis

Notebooks in applied statistics, Monte Carlo methods, and simulation — covering coursework from master studies as well as independent investigations into areas of personal interest.

---

## bayesian-reasoning

**`bayesian_inference_medical_testing.ipynb`**  
Bayesian updating applied to a disease test. Derives the posterior probability of infection given a positive result analytically, showing why a 97% accurate test still produces mostly false positives at low prevalence. Sequential updating across multiple tests.

**`bayesian_root_cause_attribution.ipynb`**  
Three suppliers producing defective parts. Given a defective part, which supplier is most likely responsible? Monte Carlo verification against analytical Bayes. Real-world connection to MLOps anomaly detection.

**`bayesian_job_search.ipynb`**  
Job search modelled as Bayesian inference. Sequential belief updating across rejections and interviews. Portfolio view: how many applications are needed to reach a target interview probability?

**`bayesian_monte_carlo_verification.ipynb`**  
Simulates 80 million people to verify the disease test result empirically. Counts all four outcomes (TP, FN, FP, TN) and compares the empirical posterior against the analytical Bayes prediction. Stability analysis across 20 independent runs.

---

## monte-carlo-methods

**`fundamental_monte_carlo_example.ipynb`**  
Estimating π by counting random raindrops in a unit square. Demonstrates the core idea of Monte Carlo integration and the 1/√N convergence rate empirically on a log-log scale.

**`monte_carlo_sampling_methods.ipynb`**  
Inverse CDF sampling for the Breit-Wigner distribution. Probability Integral Transform. Demonstration that the Breit-Wigner has an undefined mean but a well-defined median — running mean never converges, running median does.

---

## sampling-distributions

**`sampling_distributions_bin_counts.ipynb`**  
Generate 100 uniform random numbers, bin into 5 equal bins, repeat 10,000 times. Studies the distribution of a single bin count (Binomial vs Poisson vs Normal), compares variance predictions, and shows that bin counts are negatively correlated due to the fixed total constraint — with the theoretical Pearson correlation of −1/(N_bins−1) confirmed empirically.

---

## simulation-studies

**`martingale_classic_vs_modified.ipynb`**  
1,000 simulated players per strategy on a fair coin. Classic Martingale (always bet the same side) vs Modified Martingale (bet the side that would correct the running average toward 0.5). Real parameters: €0.10 minimum bet, €1,000 bankroll. Shows both strategies produce identical bankruptcy rates — the running average carries no predictive information on a fair coin.

**`wealth_distribution_pareto.ipynb`**  
Agent-based simulation of 10,000 agents over 200 years starting from perfect equality (Gini=0). Includes labour income shocks, volatile capital returns, market crashes, individual catastrophic life events, and wealth-dependent spending calibrated to Scandinavian data. Shows Pareto wealth distribution and real-world Gini values emerging from normal economic behaviour alone.

---

## Stack

Python 3 · NumPy · Matplotlib · SciPy
