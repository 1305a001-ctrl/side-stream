"""Streams Edge — paid signal channel + alerts SaaS on Chainlink Data Streams.

Two product surfaces on one backend:

  signal_pusher    Subscribes to chainlink:eval_log + tokenized_equity:eval_log
                   on the ai-primary Redis. Broadcasts qualifying signals to
                     (a) public free TG channel (5-min delay, top 3/day)
                     (b) private Signal Pro TG group (real-time, all)

  trigger_engine   Subscribes to chainlink:<alias>:reports. Evaluates per-user
                   price-cross trigger rules. Fires via webhook / Discord /
                   Slack / email on hit.

Shared:
  Postgres for user accounts, trigger rules, delivery logs, subscriptions.
  Redis for hot trigger evaluation (rules preloaded into memory).
  Whop OR custom Stripe for payments (delivery-agnostic — switch via env).

See docs/scope.md for the product spec.
"""
__version__ = "0.1.0"
