# Security policy

## Supported versions

Agoranomos is pre-1.0. Its first release, v0.1.0, came out on 11 October 2026. Until version 1.0, only the latest minor release is supported: it receives security fixes, and earlier minor releases do not.

## Reporting a vulnerability

Please report vulnerabilities privately. Do not open a public issue or pull request for them.

Use GitHub private vulnerability reporting: open the **Security** tab of the repository and choose **Report a vulnerability**. Only the maintainers can see the report.

If that option is not available, for example while the repository is private, write to the maintainer, [@andrei-gromov](https://github.com/andrei-gromov), at the email address on their GitHub profile. Do not describe the problem in public.

A good report says:

- what you found and where it is (a file, a component or a setting);
- which version or commit you tested;
- how to reproduce it, or a proof of concept;
- what an attacker could do with it, and a fix if you have one.

Use test data in your report, and remove real credentials, tokens and customer data from anything you send.

## What to expect

- We acknowledge your report within 7 days.
- We investigate, tell you what we find and work with you on a fix.
- We ask you to keep the details private until a fix is released, or until we have agreed with you on a date to disclose them.

## Scope

This policy covers vulnerabilities in Agoranomos itself: its code, its default configuration and its documentation. Vulnerabilities in the third-party software that the stack runs, such as Apache Airflow, PostgreSQL or Metabase, belong to those projects, so report them there. If Agoranomos makes such a vulnerability easier to exploit, for example through an insecure default, report that to us.
