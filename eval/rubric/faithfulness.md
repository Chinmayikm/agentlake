# Faithfulness — is the answer grounded in the retrieved chunks?

You are grading whether an answer is SUPPORTED BY the source passages it was
given. You are **not** grading whether the answer is correct in general, and
you are not grading whether it is well written. An answer that is factually
true but says something the passages do not support is unfaithful — that is
the failure this scale exists to catch.

Judge only against the passages below. If you know something they do not say,
that knowledge is not evidence here.

## Scale

**5 — Fully grounded.** Every claim in the answer is supported by the
passages. This includes an answer that correctly states the passages do not
cover the question, when they indeed do not.

**4 — Grounded, with a harmless embellishment.** Every substantive claim is
supported; the answer adds only framing, restatement, or a generic remark that
asserts nothing checkable.

**3 — Mostly grounded, one unsupported claim.** The main answer is supported,
but at least one specific factual claim — a number, a config name, a
behaviour — is not in the passages. It may still be true; it is not evidenced.

**2 — Substantially unsupported.** Several specific claims are absent from the
passages, or the central claim rests on something the passages do not say.

**1 — Contradicted or fabricated.** The answer contradicts the passages, or
invents specifics (a config key, a version, a number, an API) that appear
nowhere in them. An answer that confidently answers a question the passages do
not address AT ALL scores here.

## Calibration notes

- Length is irrelevant to this scale. A one-sentence answer that is fully
  supported scores 5.
- "The corpus does not cover this" is a **5** when the passages confirm it, and
  a **1** when the passages plainly do contain the answer.
- Do not reward hedging. "It may be around 512 MB" is still an unsupported
  specific if the passages do not give a figure.
- If the answer cites a source that the passages show does not say what is
  claimed, that is a 1 or 2, not a 3 — a wrong citation is worse than none.
