# FlexCar Metrics Engineering Analysis

## Overview
This repository contains a comprehensive metrics engineering analysis for **FlexCar**, a long-term car rental service that provides flexibility for drivers who want to switch vehicles monthly without the commitment of ownership.

## Contents

1. **[User Journey](./user-journey.md)**
   - Detailed walkthrough of the FlexCar experience from discovery to long-term usage
   - Highlights of what makes the product exceptional
   - Metrics impact at each stage of the journey

2. **[Experiments](./experiments.md)**
   - Three detailed experiment proposals to improve the product
   - Test cell allocations and conditions
   - Leading and lagging metric hypotheses
   - Statistical analysis plans

## Experiment Summary

### Experiment 1: Dynamic Pricing for Switch Surcharge
**Goal**: Increase switch adoption through flexible pricing  
**Test Cells**: Control, Tenure-based pricing, Seasonal pricing  
**Key Metrics**: Switch rate, ARPU, retention, NPS

### Experiment 2: Personalized Vehicle Recommendation Engine
**Goal**: Reduce decision fatigue and improve match quality  
**Test Cells**: Control, Top 3 recommendations, Guided quiz  
**Key Metrics**: Time to selection, completion rate, satisfaction, early returns

### Experiment 3: Gamified Loyalty Program with Milestones
**Goal**: Increase engagement and emotional investment  
**Test Cells**: Control, Basic gamification, Full gamification  
**Key Metrics**: Retention, churn, MAU, referrals, LTV

## Metrics Framework

### Leading Indicators
- User engagement metrics (page views, session duration, feature usage)
- Behavioral signals (exploration rate, initiation rate)
- Perception scores (surveys, NPS drivers)

### Lagging Indicators
- Revenue metrics (ARPU, LTV)
- Retention and churn
- Customer satisfaction (NPS, CSAT)
- Business outcomes (utilization, referrals)

## Methodology
All experiments follow rigorous statistical practices:
- Randomized controlled trials
- Stratified sampling
- Intention-to-treat analysis
- Guardrail metrics to prevent regressions
- Multiple comparison corrections


# FlexCar User Journey: A Metrics Engineer's Perspective

## Company Overview
**FlexCar** is a long-term car rental service designed for drivers who want flexibility without the commitment of purchasing a vehicle. The platform allows users to switch cars monthly for a small surcharge, providing the freedom to adapt their vehicle choice to their changing needs.

---

## User Journey: Things I Loved

### 1. **Discovery & Sign-Up (Week 1)**
**What I Loved:**
- **Transparent Pricing Model**: The pricing calculator showed me exactly what I'd pay monthly, including the flexibility surcharge. No hidden fees or surprises.
- **Quick Digital Onboarding**: Uploaded my driver's license, connected my payment method, and verified my insurance in under 10 minutes.
- **Vehicle Selection Interface**: The filtering system let me compare vehicles by:
  - Monthly cost
  - Fuel efficiency
  - Cargo space
  - Available features (Apple CarPlay, heated seats, etc.)
- **Instant Approval**: Credit check and approval happened within 2 hours, much faster than traditional car buying or long-term leasing.

**Metrics Impact:**
- Time-to-value: < 1 day from sign-up to first vehicle reservation
- Drop-off rate: Minimal during onboarding due to streamlined process

---

### 2. **First Vehicle Pickup (Week 1-2)**
**What I Loved:**
- **Flexible Pickup Locations**: Could choose from 5 locations within 10 miles of my home.
- **Digital Walk-Around**: The app guided me through a vehicle inspection checklist with photo uploads to document pre-existing damage.
- **Contactless Experience**: All keys were in a lockbox; I unlocked it with a code from the app.
- **Welcome Package**: Found a small care kit in the car (phone mount, charging cable, emergency contact card).

**Metrics Impact:**
- Pickup completion rate: 98%
- Customer satisfaction (NPS) post-pickup: High

---

### 3. **First Month Usage (Month 1)**
**What I Loved:**
- **Maintenance Included**: Oil changes and tire rotations were included. The app sent me reminders and let me schedule at partner locations.
- **Roadside Assistance Integration**: Had a flat tire once; one tap in the app connected me to 24/7 roadside assistance.
- **Usage Dashboard**: Could track my mileage against my plan limit, with proactive alerts at 70% and 90% usage.
- **Insurance Clarity**: My personal insurance covered the vehicle, but FlexCar offered supplemental coverage options clearly explained in the app.

**Metrics Impact:**
- Monthly engagement: Daily app opens to check mileage
- Support ticket volume: Low due to self-service features

---

### 4. **First Vehicle Switch (Month 2)**
**What I Loved:**
- **Switch Reminder**: Got a notification 2 weeks before my renewal asking if I wanted to keep my current car or switch.
- **Easy Comparison**: Could see what other vehicles were available in my price range, with filters for "similar to current" or "upgrade options."
- **Seamless Transition**: Scheduled return of my sedan and pickup of an SUV (needed it for a camping trip). Both happened at the same location on the same day with a 1-hour buffer.
- **No Paperwork**: The switch was processed entirely through the app. Just two digital vehicle inspections.

**Metrics Impact:**
- Switch adoption rate: Loved the flexibility; switched 40% of the time
- Customer retention: High due to unique value proposition

---

### 5. **Ongoing Usage (Month 3-12)**
**What I Loved:**
- **Loyalty Rewards**: Earned points for on-time payments and vehicle care, redeemable for free switch surcharges or mileage upgrades.
- **Seasonal Recommendations**: In winter, the app suggested AWD vehicles; in summer, convertibles were highlighted.
- **Community Features**: Could see reviews from other FlexCar users about specific vehicles and their experiences.
- **Flexible Plans**: Upgraded my mileage plan mid-month when I had an unexpected road trip (prorated cost).
- **Predictive Maintenance**: App notified me when my vehicle was due for service before any dashboard warning lights appeared.

**Metrics Impact:**
- Customer lifetime value (LTV): Increased through continued engagement
- Monthly active users (MAU): Consistently high
- Referral rate: Recommended to 3 friends who signed up

---

### 6. **Long-Term Satisfaction (Current)**
**What I Love Now:**
- **Cost Savings**: Compared to owning (depreciation, insurance, maintenance), I'm saving ~$200/month.
- **Stress-Free**: No worries about selling a car, trade-in values, or being stuck with a vehicle I don't like.
- **Lifestyle Fit**: Can switch to a truck when moving furniture, a hybrid for eco-friendly commuting, or a larger SUV for family road trips.
- **App Evolution**: Regular feature updates based on user feedback (recently added "favorite vehicles" and "quick rebook").

**Metrics Impact:**
- Net Promoter Score (NPS): 9/10 - highly likely to recommend
- Churn rate: Very low; no plans to leave the platform
- Average subscription length: Extended beyond initial expectations

---

## Key Success Factors from a Metrics Perspective

1. **Transparency**: Clear pricing and no surprises build trust
2. **Convenience**: Digital-first, contactless, and flexible
3. **Choice**: Wide vehicle selection and easy switching
4. **Reliability**: Maintenance included, roadside assistance, quality vehicles
5. **Value**: Lower cost than ownership with more flexibility than traditional rentals

---

## Areas Where Data & Metrics Enhanced Experience

- **Mileage tracking**: Prevented overages through proactive alerts
- **Predictive notifications**: Timely reminders for switches, maintenance, renewals
- **Personalization**: Vehicle recommendations based on my history and preferences
- **Quality control**: Digital inspections created accountability and transparency


# FlexCar Experiment Proposals: Metrics Engineering

## Overview
As a metrics engineer at FlexCar, I propose three experiments designed to improve user experience, increase engagement, and optimize key business metrics. Each experiment includes detailed test cell allocation, conditions, and expected metric impacts.

---

## Experiment 1: Dynamic Pricing for Switch Surcharge

### **Hypothesis**
Reducing the switch surcharge during off-peak months will increase switch adoption rate without significantly impacting revenue, while improving customer satisfaction and retention.

### **Problem Statement**
Currently, users pay a flat $50 surcharge to switch vehicles each month. Data suggests that switch rates are lower during certain months (January-March, post-holiday season) due to price sensitivity. By offering dynamic pricing, we can:
- Encourage switches during low-demand periods
- Balance vehicle inventory across locations
- Increase customer perception of value

---

### **Test Design**

#### **Test Cells & Allocation**
| Cell | Description | Allocation | Conditions |
|------|-------------|------------|------------|
| **Control** | Standard flat $50 switch surcharge | 40% | Current pricing model, no changes |
| **Test Cell A** | Tiered dynamic pricing based on tenure | 30% | - Months 1-3: $50<br>- Months 4-6: $40<br>- Months 7-12: $30<br>- Months 13+: $25 |
| **Test Cell B** | Seasonal dynamic pricing | 30% | - Peak months (June-Aug, Nov-Dec): $50<br>- Off-peak months (Jan-Mar): $30<br>- Standard months (Apr-May, Sep-Oct): $40 |

#### **Randomization**
- Users randomized at account creation
- Stratified by:
  - Geographic region (to account for seasonal differences)
  - Plan type (mileage tier)
  - Historical switch behavior (if existing user)

#### **Duration**
- 6 months (to capture full seasonal cycle)
- Minimum sample size: 15,000 users per cell (45,000 total)

---

### **Success Metrics**

#### **Leading Indicators** (Observable within 1-2 months)
1. **Switch Exploration Rate**
   - **Metric**: % of users who view the "Switch Vehicle" page
   - **Hypothesis**: Test cells will show 15-20% increase in exploration
   - **Tracking**: App analytics on page views and time spent

2. **Switch Initiation Rate**
   - **Metric**: % of users who start the switch process (add vehicle to cart)
   - **Hypothesis**: Test Cell B will show 25% increase during off-peak months
   - **Tracking**: Funnel analysis from exploration → initiation

3. **Pricing Perception Score**
   - **Metric**: Survey question "FlexCar pricing is fair and transparent" (1-5 scale)
   - **Hypothesis**: Test cells will score 0.3-0.5 points higher
   - **Tracking**: In-app survey sent monthly

4. **Customer Support Contacts About Pricing**
   - **Metric**: Number of tickets mentioning "switch cost" or "surcharge"
   - **Hypothesis**: Test cells will show 20% reduction in pricing-related inquiries
   - **Tracking**: Support ticket categorization and sentiment analysis

---

#### **Lagging Indicators** (Observable within 3-6 months)
1. **Switch Adoption Rate**
   - **Metric**: % of eligible users who complete at least one switch per 6 months
   - **Current Baseline**: 35%
   - **Hypothesis**: 
     - Test Cell A: 42% (20% increase)
     - Test Cell B: 45% (28% increase)
   - **Tracking**: Monthly cohort analysis

2. **Average Switches Per User Per Year**
   - **Metric**: Total switches / active users / year
   - **Current Baseline**: 1.8 switches/year
   - **Hypothesis**: 
     - Test Cell A: 2.3 switches/year
     - Test Cell B: 2.5 switches/year
   - **Tracking**: Aggregated usage data

3. **Customer Retention Rate (6-month cohort)**
   - **Metric**: % of users still active after 6 months
   - **Current Baseline**: 78%
   - **Hypothesis**: 
     - Test Cell A: 82% (loyal customers feel rewarded)
     - Test Cell B: 80% (seasonal flexibility reduces churn)
   - **Tracking**: Cohort retention analysis

4. **Net Revenue Per User (ARPU)**
   - **Metric**: (Monthly subscription + switch surcharges) / active users
   - **Current Baseline**: $385/month
   - **Hypothesis**:
     - Test Cell A: $380/month (slight decrease, offset by retention)
     - Test Cell B: $390/month (increased switches offset lower per-switch cost)
   - **Tracking**: Financial reporting with cohort segmentation

5. **Net Promoter Score (NPS)**
   - **Metric**: "How likely are you to recommend FlexCar?" (0-10)
   - **Current Baseline**: NPS = 42
   - **Hypothesis**: 
     - Test Cell A: NPS = 48 (tenure rewards appreciated)
     - Test Cell B: NPS = 50 (seasonal flexibility highly valued)
   - **Tracking**: Quarterly NPS survey

6. **Vehicle Utilization Rate**
   - **Metric**: % of fleet actively rented at any given time
   - **Current Baseline**: 82%
   - **Hypothesis**: Test Cell B will smooth demand → 85% utilization
   - **Tracking**: Fleet management dashboard

---

### **Guardrail Metrics** (Must Not Degrade)
- Customer support ticket volume: Should not increase >5%
- Average days to switch completion: Should remain ≤3 days
- Vehicle damage incident rate: Should not increase (switching doesn't reduce care)

---

### **Analysis Plan**
- **Primary analysis**: Intention-to-treat (ITT) using assigned cohort
- **Secondary analysis**: Per-protocol analysis (users who actively switched)
- **Statistical method**: Two-proportion z-test for switch rate; t-test for continuous metrics
- **Significance level**: α = 0.05
- **Multiple comparison correction**: Bonferroni correction for multiple test cells

---

## Experiment 2: Personalized Vehicle Recommendation Engine

### **Hypothesis**
An AI-powered recommendation engine that suggests vehicles based on user behavior, preferences, and context will increase switch satisfaction, reduce decision fatigue, and improve retention.

### **Problem Statement**
Users currently browse an average of 12 vehicles before selecting one to switch to, spending 25+ minutes in the app. Many users report feeling overwhelmed by choice. A personalized recommendation system can:
- Surface relevant vehicles faster
- Reduce cognitive load
- Improve match quality between user needs and vehicle selection

---

### **Test Design**

#### **Test Cells & Allocation**
| Cell | Description | Allocation | Conditions |
|------|-------------|------------|------------|
| **Control** | Current UI: Full catalog with basic filters | 40% | - No recommendations<br>- Standard sorting (price, popularity, availability)<br>- Manual filtering by user |
| **Test Cell A** | "Top 3 Picks" recommendation banner | 30% | - ML model suggests 3 vehicles<br>- Appears at top of browse page<br>- Based on: past rentals, current season, reviews, mileage usage |
| **Test Cell B** | Guided quiz + recommendations | 30% | - 5-question quiz on switch intent page:<br>  1. Purpose of switch? (work, leisure, moving, etc.)<br>  2. Passengers? (solo, couple, family)<br>  3. Distance? (city, highway, road trip)<br>  4. Priority? (cost, features, eco-friendly)<br>  5. Open to trying something new? (yes/no)<br>- Top 5 personalized results shown first<br>- Can still browse full catalog |

#### **Randomization**
- Users randomized when clicking "Switch Vehicle" button
- Stratified by:
  - Number of previous switches (0, 1-2, 3+)
  - Average time spent browsing in past sessions
  - Account age

#### **Duration**
- 3 months
- Minimum sample size: 10,000 users per cell (30,000 total)

---

### **Success Metrics**

#### **Leading Indicators** (Observable within 2-4 weeks)
1. **Time to Vehicle Selection**
   - **Metric**: Minutes from "Switch Vehicle" click to "Reserve" click
   - **Current Baseline**: 28 minutes
   - **Hypothesis**:
     - Test Cell A: 20 minutes (29% reduction)
     - Test Cell B: 15 minutes (46% reduction)
   - **Tracking**: Session analytics with timestamps

2. **Vehicles Viewed Per Session**
   - **Metric**: Number of vehicle detail pages opened
   - **Current Baseline**: 12 vehicles
   - **Hypothesis**:
     - Test Cell A: 8 vehicles (recommendation banner reduces browsing)
     - Test Cell B: 5 vehicles (quiz narrows options effectively)
   - **Tracking**: Page view analytics

3. **Recommendation Click-Through Rate (CTR)**
   - **Metric**: % of users who click on a recommended vehicle
   - **Hypothesis**:
     - Test Cell A: 65% CTR on "Top 3 Picks"
     - Test Cell B: 85% CTR on quiz results
   - **Tracking**: Event tracking on recommendation clicks

4. **Recommendation Conversion Rate**
   - **Metric**: % of recommended vehicles that are ultimately reserved
   - **Hypothesis**:
     - Test Cell A: 40% of switches are from recommended vehicles
     - Test Cell B: 60% of switches are from recommended vehicles
   - **Tracking**: Attribution of reserved vehicle to recommendation source

---

#### **Lagging Indicators** (Observable within 1-3 months)
1. **Switch Completion Rate**
   - **Metric**: % of "Switch Vehicle" sessions that result in completed reservation
   - **Current Baseline**: 68%
   - **Hypothesis**:
     - Test Cell A: 73% (reduced friction)
     - Test Cell B: 78% (guided experience reduces abandonment)
   - **Tracking**: Conversion funnel analysis

2. **Post-Switch Satisfaction Score**
   - **Metric**: "How satisfied are you with your new vehicle?" (1-5 scale)
   - **Current Baseline**: 4.1
   - **Hypothesis**:
     - Test Cell A: 4.3 (better matches)
     - Test Cell B: 4.5 (quiz ensures alignment with needs)
   - **Tracking**: Automated survey 1 week after switch

3. **Early Return Rate**
   - **Metric**: % of users who switch again within 2 weeks (indicator of poor match)
   - **Current Baseline**: 5%
   - **Hypothesis**:
     - Test Cell A: 3.5% (better initial match)
     - Test Cell B: 2% (quiz prevents mismatches)
   - **Tracking**: Switch frequency analysis with <14 day filter

4. **Repeat Switch Rate**
   - **Metric**: % of users who switch again within next 6 months
   - **Current Baseline**: 45%
   - **Hypothesis**:
     - Test Cell A: 50% (positive experience encourages future switches)
     - Test Cell B: 55% (quiz makes switching easier and more enjoyable)
   - **Tracking**: Cohort analysis of switching behavior

5. **Customer Lifetime Value (LTV)**
   - **Metric**: Total revenue from user over lifetime
   - **Current Baseline**: $4,620 (12-month average tenure)
   - **Hypothesis**:
     - Test Cell A: $5,100 (increased tenure due to satisfaction)
     - Test Cell B: $5,400 (quiz creates stickiness)
   - **Tracking**: LTV modeling with cohort comparison

6. **App Engagement Score**
   - **Metric**: Composite of app opens, time spent, features used
   - **Hypothesis**: Test cells show higher engagement due to personalization
   - **Tracking**: Product analytics dashboard

---

### **Guardrail Metrics**
- Recommendation model bias: No vehicle type should be over-recommended >150% of its availability
- User perception of "creepiness": Survey question "Recommendations felt intrusive" <10%
- Technical performance: Page load time should not increase >500ms

---

### **Analysis Plan**
- **Primary analysis**: Switch completion rate (ITT)
- **Secondary analysis**: Satisfaction and retention metrics
- **A/B/C test**: Statistical comparison using ANOVA with post-hoc tests
- **Segmentation**: Analyze by user tenure, past switch frequency, engagement level
- **Qualitative**: User interviews with 20 users per cell to understand experience

---

## Experiment 3: Gamified Loyalty Program with Milestones

### **Hypothesis**
Introducing a gamified loyalty program with visible progress milestones will increase user engagement, reduce churn, and create emotional investment in the FlexCar brand.

### **Problem Statement**
While FlexCar's current loyalty program offers points for on-time payments and vehicle care, it lacks visibility and excitement. Users don't regularly engage with it. A gamified approach with clear milestones can:
- Increase awareness of loyalty benefits
- Encourage desired behaviors (vehicle care, timely returns, referrals)
- Create emotional connection and brand affinity

---

### **Test Design**

#### **Test Cells & Allocation**
| Cell | Description | Allocation | Conditions |
|------|-------------|------------|------------|
| **Control** | Current loyalty program (points in background) | 33% | - Earn points passively<br>- Must navigate to "Rewards" page to see balance<br>- No notifications or milestones |
| **Test Cell A** | Basic gamification: Levels & badges | 33% | - 5 levels: Explorer (0-3mo), Navigator (4-6mo), Voyager (7-12mo), Pathfinder (13-18mo), Legend (19mo+)<br>- Earn badges: "Perfect Return," "Eco Warrior" (hybrid), "Adventurer" (3+ switches), etc.<br>- Progress bar on home screen<br>- Push notification when leveling up |
| **Test Cell B** | Full gamification: Levels, badges, challenges & leaderboard | 34% | - Everything in Test Cell A, plus:<br>- Monthly challenges: "Try a new vehicle type," "Refer a friend," "Complete a switch in <30 days"<br>- Local leaderboard: See ranking vs. users in your city (anonymized)<br>- Unlockable perks: Free switch at Level 3, priority vehicle selection at Level 4, exclusive vehicle access at Level 5<br>- Social sharing: Share badges on social media |

#### **Randomization**
- New users randomized at account creation
- Existing users: Stratify by tenure and randomly assign
  - For existing users, backfill progress based on historical data

#### **Duration**
- 6 months (sufficient time to progress through levels)
- Minimum sample size: 20,000 users per cell (60,000 total)

---

### **Success Metrics**

#### **Leading Indicators** (Observable within 2-4 weeks)
1. **Loyalty Program Awareness**
   - **Metric**: % of users who can correctly identify their current level/status
   - **Current Baseline**: 22% (poor awareness)
   - **Hypothesis**:
     - Test Cell A: 65% (visible progress bar)
     - Test Cell B: 80% (challenges drive engagement)
   - **Tracking**: In-app survey

2. **Rewards Page Visits**
   - **Metric**: % of MAU who visit rewards/loyalty page monthly
   - **Current Baseline**: 18%
   - **Hypothesis**:
     - Test Cell A: 45% (levels create curiosity)
     - Test Cell B: 60% (challenges and leaderboard)
   - **Tracking**: Page view analytics

3. **Push Notification Engagement**
   - **Metric**: Open rate on loyalty-related notifications
   - **Hypothesis**:
     - Test Cell A: 35% open rate (level-up notifications)
     - Test Cell B: 45% open rate (challenge notifications)
   - **Tracking**: Push notification analytics

4. **Challenge Participation Rate** (Test Cell B only)
   - **Metric**: % of users who attempt at least one challenge per month
   - **Hypothesis**: 40% monthly participation
   - **Tracking**: Challenge completion events

---

#### **Lagging Indicators** (Observable within 3-6 months)
1. **Customer Retention Rate (6-month cohort)**
   - **Metric**: % of users still active after 6 months
   - **Current Baseline**: 78%
   - **Hypothesis**:
     - Test Cell A: 82% (sunk cost fallacy; invested in progress)
     - Test Cell B: 85% (unlockable perks create lock-in)
   - **Tracking**: Cohort retention analysis

2. **Churn Rate**
   - **Metric**: % of users who cancel subscription monthly
   - **Current Baseline**: 4.5% per month
   - **Hypothesis**:
     - Test Cell A: 3.8% (15% reduction)
     - Test Cell B: 3.2% (29% reduction)
   - **Tracking**: Monthly churn reporting by cohort

3. **Average Session Duration**
   - **Metric**: Minutes per app session
   - **Current Baseline**: 4.2 minutes
   - **Hypothesis**:
     - Test Cell A: 5.5 minutes (exploring badges)
     - Test Cell B: 6.8 minutes (challenges and leaderboard)
   - **Tracking**: Session analytics

4. **Monthly Active Users (MAU)**
   - **Metric**: % of subscribers who open app at least once per month
   - **Current Baseline**: 85%
   - **Hypothesis**:
     - Test Cell A: 88% (notifications bring users back)
     - Test Cell B: 91% (challenges require regular check-ins)
   - **Tracking**: Monthly engagement reporting

5. **Referral Rate**
   - **Metric**: % of users who refer at least one friend
   - **Current Baseline**: 12%
   - **Hypothesis**:
     - Test Cell A: 15% (badge incentive)
     - Test Cell B: 22% (challenge + leaderboard + social sharing)
   - **Tracking**: Referral attribution

6. **Net Promoter Score (NPS)**
   - **Metric**: Likelihood to recommend (0-10 scale)
   - **Current Baseline**: NPS = 42
   - **Hypothesis**:
     - Test Cell A: NPS = 46 (fun factor increases sentiment)
     - Test Cell B: NPS = 52 (emotional investment in brand)
   - **Tracking**: Quarterly NPS survey by cohort

7. **Customer Lifetime Value (LTV)**
   - **Metric**: Total revenue over customer lifetime
   - **Current Baseline**: $4,620 (12-month average tenure)
   - **Hypothesis**:
     - Test Cell A: $5,200 (extended tenure)
     - Test Cell B: $5,900 (extended tenure + more switches via challenges)
   - **Tracking**: LTV modeling with cohort comparison

---

### **Guardrail Metrics**
- Negative sentiment: Monitor for "too gimmicky" or "annoying" feedback <5%
- Competitive behavior: Leaderboard should not create toxic behavior (monitor support tickets)
- Reward redemption rate: Should increase (indicates perceived value)

---

### **Analysis Plan**
- **Primary analysis**: 6-month retention rate (ITT)
- **Secondary analysis**: Engagement and NPS metrics
- **Segmentation**: Analyze by age group (younger users may respond more to gamification), tenure, engagement level
- **Qualitative**: Focus groups with power users and churned users to understand motivations
- **Longitudinal**: Track cohorts over 12 months to see long-term impact

---

## Experiment Success Criteria & Rollout Plan

### **Decision Framework**
Each experiment will be evaluated holistically:

1. **Experiment 1 (Dynamic Pricing)**: 
   - **Ship if**: Switch rate increases ≥15% AND revenue per user decreases <5%
   - **Iterate if**: One test cell outperforms but not significantly
   - **Kill if**: Revenue impact is severe (>10% decrease) without offsetting retention gains

2. **Experiment 2 (Recommendations)**:
   - **Ship if**: Switch completion rate increases ≥5% AND satisfaction score improves ≥0.2
   - **Iterate if**: One cell shows promise; refine ML model or quiz questions
   - **Kill if**: No improvement in completion rate OR increased early returns (poor matches)

3. **Experiment 3 (Gamification)**:
   - **Ship if**: 6-month retention improves ≥4% AND MAU increases ≥3%
   - **Iterate if**: Some elements work (e.g., badges yes, leaderboard no)
   - **Kill if**: Negative sentiment >10% OR no retention impact

### **Rollout Strategy**
- **Phase 1**: Run all experiments simultaneously (different user cohorts)
- **Phase 2**: Analyze results after minimum duration
- **Phase 3**: Ship winning variants to 100% of users gradually (10% → 50% → 100% over 4 weeks)
- **Phase 4**: Monitor metrics post-launch for regressions

---

## Instrumentation & Data Infrastructure Requirements

### **Data Collection**
- Event tracking: User actions (clicks, page views, completions)
- User properties: Demographics, tenure, plan type, behavior flags
- Custom events: Switch initiated, recommendation clicked, challenge completed, badge earned

### **Dashboards**
- Real-time experiment dashboard: Metric trends by cohort
- Alerting: Automated alerts if guardrail metrics degrade
- Drill-down: Ability to segment by user attributes

### **Analytics Tools**
- A/B testing platform: Statsig, Optimizely, or custom solution
- Product analytics: Mixpanel, Amplitude, or equivalent
- SQL warehouse: Snowflake, BigQuery for deep dives
- BI visualization: Tableau, Looker for stakeholder reporting

---

## Ethical Considerations

1. **Pricing Experiment**: Ensure fairness; no discrimination based on protected attributes
2. **Recommendation Engine**: Audit for bias; ensure diverse vehicle types are recommended
3. **Gamification**: Avoid dark patterns; users should feel rewarded, not manipulated
4. **Data Privacy**: All experiments comply with GDPR, CCPA; user data anonymized for analysis

---

## Conclusion

These three experiments represent a comprehensive approach to improving FlexCar's user experience through data-driven product development. Each experiment:
- Addresses a real user pain point
- Has clear, measurable success criteria
- Balances leading and lagging indicators
- Considers both user experience and business outcomes

By running these experiments, FlexCar can evolve from a functional car rental service to a delightful, personalized, and engaging platform that keeps users coming back month after month.