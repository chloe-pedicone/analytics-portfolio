# Hospital call center (HCC): productivity and staffing case study

**Type:** Case study (Sept 2023) · **Tools:** Python (pandas, matplotlib/seaborn), PowerPoint
**Data:** Agent activity at half-hour intervals for Aug 2022: logged-in time, not-ready time, handled calls and handle time, plus an agent-to-team lookup. Agents and teams are anonymized (`Agent 1`, `team_1`).

## Questions
- What does the call center's current state look like overall, by team and by agent?
- How do logged-in time, talk time and not-ready time split across teams?
- What are the utilization and non-utilization rates, and how do they trend?
- Who are the top and bottom performers by calls handled?
- How should a manager allocate FTEs across the week, based on utilization?
- How can productivity be defined as a model, at team and agent level?

## Files
| File | What it is |
|---|---|
| [`hcc-case-study-notebook.pdf`](hcc-case-study-notebook.pdf) | Full analysis notebook (PDF export) |
| [`hcc-case-study-notebook.html`](hcc-case-study-notebook.html) | Same notebook as HTML |
| [`hcc-case-report-slides.pptx`](hcc-case-report-slides.pptx) | 30-slide presentation for the call-center manager |

## How it connects to my current work
This was my first contact-center analysis. Today I build the production version of these metrics (service level, handle time, business-hours rules) on Microsoft Fabric. See [CS1](../../case-studies/01-contact-center-kpi-model/).
