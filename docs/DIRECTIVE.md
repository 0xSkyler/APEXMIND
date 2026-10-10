# APEX MIND V4 — PROVEN ALPHA DEVELOPMENT DIRECTIVE

## Mission

Develop a measurable competitive trading advantage for an autonomous Lighter.xyz perpetual futures system.

The objective is to discover, validate, and execute statistically supported opportunities that produce positive expected net returns after realistic costs, funding, execution delay, and downside risk.

Do not assume the existence of a profitable strategy.

Do not declare competitive superiority without evidence.

The initial research priority is:

**Cross-Venue Informed Flow Alpha (CVIFA)**

This strategy combines cross-venue price discovery, Lighter order-book microstructure, trade-flow analysis, and execution-conditioned return forecasting.

## 1. Data Acquisition

Build a synchronized collector for:

- Lighter real-time best bid and ask.
- Lighter level-two order-book updates.
- Lighter executed trades.
- Lighter funding and market statistics.
- Lighter account-specific execution timestamps.
- Reference exchange best bid and ask.
- Reference exchange order-book updates.
- Reference exchange trades and prices.

Start with liquid instruments available on both Lighter and the selected reference venue.

Expand to all eligible markets after confirming correct instrument mapping and data quality.

Use monotonic local arrival timestamps alongside original exchange event timestamps.

Measure and monitor clock synchronization errors and feed delays.

Do not treat asynchronous market events as simultaneous merely because they fall within the same one-second interval.

## 2. Feature Engineering

Calculate time-aware features including:

- Cross-venue return difference.
- Normalized price-basis deviation.
- Reference-market return over multiple short horizons.
- Lighter-market return over the same horizons.
- Order-book imbalance.
- Order-flow imbalance.
- Aggressive buy/sell volume ratio.
- Spread.
- Available order-book depth.
- Estimated market impact.
- Short-term realized volatility.
- Liquidity replenishment.
- Quote cancellation intensity where observable.
- Funding exposure.
- Market regime.
- Relative data freshness.
- Measured execution latency.

Only use features available at decision time.

Implement statistical feature selection to eliminate redundant or nonpredictive variables.

## 3. Forecast Target

Do not train the model merely to predict the next candle direction.

Train it to estimate expected net returns conditional on future executable entry and exit prices.

Model:

- Probability of a favorable outcome.
- Expected net return.
- Expected adverse movement.
- Return quantiles.
- Probability of significant slippage.
- Expected execution delay.
- Expected holding period.
- Downside risk.

Use experimentally measured exchange latency distributions, not an assumed zero-delay execution model.

## 4. Alpha Models

Benchmark several approaches:

1. Simple reference-market momentum.
2. Cross-venue lag-only signals.
3. Order-flow-only signals.
4. Cross-venue lag plus order-flow confirmation.
5. Regularized statistical models.
6. Gradient-boosted models.
7. More complex temporal models only when justified.

Compare all alternatives using identical chronological evaluation periods.

Do not deploy a complex model if a simpler model performs as well.

## 5. Strategy Decisions

For each opportunity:

1. Verify instrument eligibility.
2. Verify synchronized market data.
3. Detect whether cross-venue price discovery provides a potential signal.
4. Estimate continuation or reversal probabilities.
5. Estimate the trade's executable return distribution.
6. Subtract realistic transaction, funding, and execution costs.
7. Apply uncertainty-aware expected-value criteria.
8. Evaluate the impact on existing positions.
9. Determine feasible notional exposure and leverage.
10. Submit the trade only if its expected portfolio contribution is favorable.

Long and short decisions must be symmetric in analytical rigor, not necessarily identical in calibrated thresholds.

Entry and exit criteria must be learned from training data and validated on unseen observations.

## 6. Execution Alpha

Build an exchange-native execution optimizer.

It determines whether to use a supported passive or aggressive order based on:

- Expected fill probability.
- Remaining signal lifetime.
- Spread.
- Estimated slippage.
- Expected adverse selection.
- Available liquidity.
- Account-tier latency.
- Current transaction capacity.

Do not assume a theoretical signal can be captured at its observation-time price.

Record all realized execution outcomes.

## 7. Capital Allocation

Optimize expected risk-adjusted portfolio growth, subject to margin and solvency requirements.

The system must automatically adapt to all feasible account balances starting from $10.

Do not hardcode a minimum initial deposit above $10.

Do not automatically use greater leverage to overcome insufficient order notional.

The intelligence chooses market, direction, size, leverage, entry, and exit autonomously.

Allow multiple simultaneous positions when justified by portfolio-level analysis.

## 8. Continuous Research

Run a resource-controlled Alpha Laboratory that:

- Generates hypotheses.
- Builds predictive features.
- Tests models.
- Measures statistical uncertainty.
- Detects overfitting.
- Compares alternatives.
- Evaluates regime changes.
- Retires deteriorating strategies.
- Researches new opportunities.

Use champion-challenger model governance.

Research can continue 24/7, but model promotion requires reproducible automated validation.

## 9. Proof of Advantage

Create an independently reproducible evaluation report.

It must include:

- Exact data period.
- Market and instrument coverage.
- Total tested signals.
- Executed and rejected opportunities.
- Gross and net returns.
- Fill assumptions.
- Observed latency.
- Slippage.
- Funding costs.
- Profit factor.
- Trade expectancy.
- Maximum drawdown.
- Expected Shortfall.
- Statistical confidence intervals.
- Market regime breakdown.
- Comparison against simple trading baselines.
- Results across separate out-of-sample periods.

Report whether the model beats the baselines after costs.

Do not claim profitability from backtesting alone.

For live deployment, require successful exchange integration tests, account authorization, transaction reconciliation, and automated margin protection.

Continue measuring actual execution performance after deployment.

If the live advantage disappears, reduce or suspend the affected strategy rather than increasing leverage to compensate.

## 10. Strategic Expansion

Only after the primary alpha strategy demonstrates credible economic value should the research engine prioritize additional trading families:

- Liquidity-shock mean reversion.
- Volatility-regime momentum.
- Funding-aware positioning.
- Cross-asset relative value.
- Event-driven strategies.
- Liquidity-provision strategies.

Every new strategy must independently demonstrate value after costs.

Do not add strategies simply to increase trade frequency.

## 11. Operating Requirements

The finished system must:

- Run continuously on Ubuntu.
- Operate without an open SSH session.
- Support all eligible Lighter perpetual markets.
- Start with balances as low as $10 when exchange minimums permit trading.
- Have no arbitrary daily trade-count restriction.
- Support concurrent positions.
- Make ordinary trading decisions autonomously.
- Respect exchange request and transaction limits.
- Persist account and order state.
- Recover after interruptions.
- Maintain accurate execution and profit records.
- Protect credentials and prohibit autonomous withdrawals.

## Final instruction

The competitive advantage must come from measurable information, superior conditional forecasting, intelligent portfolio allocation, and actual execution efficiency.

Do not rely on the assumption that more AI models, more leverage, or more trading frequency automatically create greater profitability.

**The system must earn the right to deploy each strategy through verified evidence.**

Deliver working code, a reproducible research process, an execution-aware evaluation, and accurate results.

No fabricated trading history, unverified profitability claims, or unsupported promises of market outperformance.