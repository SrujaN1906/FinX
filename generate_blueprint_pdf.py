import os
import sys
from fpdf import FPDF

class BlueprintPDF(FPDF):
    def header(self):
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(120, 120, 120)
        self.cell(0, 8, 'FinX | Major Project Blueprint (v2.0 Post-Implementation)', border=0, align='L')
        self.cell(0, 8, 'Real-Time Financial Insight & Anomaly Detection', border=0, align='R', new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(220, 220, 220)
        self.line(10, 17, 200, 17)
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_draw_color(220, 220, 220)
        self.line(10, self.get_y() - 2, 200, self.get_y() - 2)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(120, 120, 120)
        self.cell(0, 10, f'Page {self.page_no()}', align='C')

    def chapter_title(self, title):
        self.set_font('Helvetica', 'B', 14)
        self.set_text_color(24, 43, 73)
        self.cell(0, 10, title, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(79, 70, 229)
        self.set_line_width(0.6)
        self.line(self.get_x(), self.get_y() - 2, self.get_x() + 60, self.get_y() - 2)
        self.set_line_width(0.2)
        self.ln(2)

    def section_title(self, title):
        self.set_font('Helvetica', 'B', 11)
        self.set_text_color(40, 50, 70)
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body_text(self, text):
        self.set_font('Helvetica', '', 9.5)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5.2, text)
        self.ln(2)

    def bullet_point(self, title, desc):
        self.set_font('Helvetica', 'B', 9.5)
        self.set_text_color(30, 30, 30)
        self.cell(5, 5, chr(149), align='R')
        self.cell(2, 5, ' ')
        self.set_font('Helvetica', 'B', 9.5)
        title_width = self.get_string_width(title + ': ')
        self.cell(title_width, 5, title + ': ')
        self.set_font('Helvetica', '', 9.5)
        self.multi_cell(0, 5, desc)
        self.ln(1)

def generate_pdf():
    pdf = BlueprintPDF()
    pdf.set_auto_page_break(auto=True, margin=18)
    pdf.add_page()

    # Cover Header
    pdf.set_font('Helvetica', 'B', 22)
    pdf.set_text_color(15, 23, 42)
    pdf.cell(0, 12, 'FinX', align='C', new_x="LMARGIN", new_y="NEXT")
    
    pdf.set_font('Helvetica', 'B', 13)
    pdf.set_text_color(79, 70, 229)
    pdf.cell(0, 8, 'Real-Time Financial Insight and Anomaly Detection System', align='C', new_x="LMARGIN", new_y="NEXT")
    pdf.set_font('Helvetica', '', 11)
    pdf.set_text_color(100, 116, 139)
    pdf.cell(0, 6, 'Using Big Data Analytics & Machine Learning', align='C', new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, 'MAJOR PROJECT BLUEPRINT -- UPDATED IMPLEMENTATION SPECIFICATION (v2.0)', align='C', new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    # Reference box
    pdf.set_fill_color(248, 250, 252)
    pdf.set_draw_color(203, 213, 225)
    pdf.rect(10, pdf.get_y(), 190, 18, 'DF')
    pdf.set_xy(12, pdf.get_y() + 2)
    pdf.set_font('Helvetica', 'I', 8.5)
    pdf.set_text_color(51, 65, 85)
    pdf.multi_cell(186, 4.5, 'Reference Purpose: This document defines the post-implementation baseline blueprint for the FinX major project. It incorporates all completed engineering deliverables including Apache Kafka ingestion, Spark Structured Streaming, Isolation Forest ML anomaly scoring, real-time multi-currency normalization to INR, PostgreSQL/SQLite persistence, and an interactive executive web dashboard.')
    pdf.set_xy(10, pdf.get_y() + 6)

    # Project Snapshot Table
    pdf.chapter_title('Project Snapshot')
    snapshot_data = [
        ('Product Name', 'FinX'),
        ('Academic Title', 'Real-Time Financial Insight & Anomaly Detection System Using Big Data Analytics'),
        ('Target Outcome', 'A demonstrable, near-real-time financial stream analytics and explainable anomaly detection prototype with INR multi-currency normalization.'),
        ('Implemented Stack', 'Kafka 7.4.0 + Zookeeper; Spark Structured Streaming; Python + Scikit-Learn (Isolation Forest); PostgreSQL + SQLite fallback; REST API + SSE; HTML5/CSS Glassmorphic Dashboard.'),
        ('Data Sources', 'Controlled Dataset (10 txns with seeded anomaly); Scale Dataset (10k txns); Custom user CSV upload with dynamic schema mapping.'),
        ('Initial ML Model', 'Isolation Forest v1.0, trained on 5 engineered features [amount_inr, log_amount, cat_code, ch_code, hour] with calibrated 0.0-1.0 scoring.'),
        ('End-to-End Flow', 'Transaction Stream -> Kafka Broker -> Spark/Python Streaming -> Currency Normalizer (INR) -> ML Isolation Forest -> PostgreSQL/SQLite -> REST/SSE -> Executive Dashboard.')
    ]

    pdf.set_font('Helvetica', '', 8.5)
    for label, val in snapshot_data:
        pdf.set_fill_color(241, 245, 249)
        pdf.set_font('Helvetica', 'B', 8.5)
        pdf.set_text_color(30, 41, 59)
        pdf.cell(45, 7, label, border=1, fill=True)
        pdf.set_fill_color(255, 255, 255)
        pdf.set_font('Helvetica', '', 8.5)
        pdf.multi_cell(145, 7, val, border=1, fill=True)
    pdf.ln(4)

    # 1. Problem Statement
    pdf.chapter_title('1. Problem Statement')
    pdf.body_text('Financial institutions process millions of continuous digital transactions across diverse channels. Traditional batch-oriented review systems suffer from high detection latency, allowing financial threats to go unnoticed until hours or days later. Furthermore, cross-border payments introduce currency conversion vulnerabilities: fixed regional monetary thresholds are easily bypassed when transactions occur in foreign currencies whose numerical values appear deceptively low before conversion.')
    pdf.body_text('FinX resolves these challenges as a demonstrable academic prototype. It ingests continuous transactions as streaming events, normalizes all international currencies strictly into Indian Rupees (INR) using live exchange rates, scores transactions via an unsupervised Isolation Forest model, records audit trails in a relational system of record, and renders explainable insights on a live executive dashboard.')

    # 2. Objectives
    pdf.chapter_title('2. Objectives')
    pdf.bullet_point('Distributed Ingestion', 'Ingest high-throughput transaction events using Apache Kafka containerized via Docker Compose.')
    pdf.bullet_point('Stream Analytics', 'Process streams with Spark Structured Streaming and Python workers to validate schemas and compute rolling aggregates.')
    pdf.bullet_point('Multi-Currency Normalization', 'Dynamically convert all international currencies (USD, EUR, GBP, AED, SGD, CAD, etc.) into base INR to guarantee zero-bypass risk scoring.')
    pdf.bullet_point('Machine Learning Detection', 'Deploy an Isolation Forest model to detect multivariate outliers, scoring records on a calibrated 0.0 to 1.0 scale with severity tiers.')
    pdf.bullet_point('System of Record Persistence', 'Persist every transaction, anomaly alert, and aggregate in PostgreSQL with automatic zero-configuration SQLite fallback.')
    pdf.bullet_point('Reactive Dashboard', 'Expose clean REST and Server-Sent Events (SSE) endpoints driving a glassmorphic dark-theme executive web interface.')
    pdf.bullet_point('Repeatable Demonstration', 'Provide guaranteed 1-click test repeatability via pre-configured controlled data, 10k scale stress data, and custom CSV upload.')

    # 3. To-Be-Remembered Points
    pdf.chapter_title('3. To-Be-Remembered Principles')
    pdf.bullet_point('Academic Prototype', 'FinX proves end-to-end architectural integrity and stream processing, not commercial fraud liability.')
    pdf.bullet_point('Synthetic & Public Data', 'Strictly uses synthetic or public transaction datasets. No real PII or customer financial secrets are stored.')
    pdf.bullet_point('Canonical Schema Contract', 'Standardized 8-field event schema (transaction_id, account_id, timestamp, original_amount, currency, amount_inr, merchant, channel, category).')
    pdf.bullet_point('Attention Signal Only', 'Flagged transactions represent statistical outliers under data features, not confirmed fraud verdicts.')
    pdf.bullet_point('Strict Decoupling', 'The web frontend consumes REST and SSE endpoints; it never connects directly to Kafka brokers or Spark runtimes.')

    # 4. Architecture
    pdf.chapter_title('4. System Architecture & Layer Responsibilities')
    pdf.body_text('FinX implements strict architectural separation across four decoupled tiers:')
    pdf.bullet_point('Ingestion Layer', 'Apache Kafka broker (localhost:9092, topic finx-transactions) decouples event producers from downstream analytics.')
    pdf.bullet_point('Analytics Engine', 'Spark Structured Streaming and Python ingestion workers process windowed aggregates and compute category metrics.')
    pdf.bullet_point('Forex Engine', 'Frankfurter API integration continuously streams live exchange rates, enforcing INR-normalized risk evaluation.')
    pdf.bullet_point('Machine Learning', 'Python Scikit-Learn Isolation Forest scores transactions across a 5-dimensional feature space.')
    pdf.bullet_point('Persistence Layer', 'PostgreSQL enterprise schema with automatic SQLite fallback (finx_records.db) guarantees persistent data integrity.')
    pdf.bullet_point('Presentation Layer', 'Multi-threaded HTTP/SSE server powers a modern glassmorphic dashboard with 60fps canvas charts and payload inspector.')

    # 5. Four Core Modules
    pdf.add_page()
    pdf.chapter_title('5. The Four Core Modules')
    modules_data = [
        ('Module 1: Data Ingestion', 'Apache Kafka 7.4.0, Zookeeper, Python Ingestion Engine', 'Ingests validated transaction events from controlled sets, 10k scale streams, and custom CSV uploads with auto-schema adaptation.'),
        ('Module 2: Stream Analytics', 'Spark Structured Streaming, Python streaming workers', 'Performs windowed aggregations, rolling throughput monitoring, category volume summaries, and database ingestion.'),
        ('Module 3: Anomaly Detection', 'Python 3.14, Scikit-Learn Isolation Forest v1.0, Joblib', 'Computes continuous anomaly scores (0.0-1.0), assigns severity tiers (LOW, MEDIUM, HIGH, CRITICAL), and generates explainable rationales.'),
        ('Module 4: API & Dashboard', 'Python HTTP & SSE Server, HTML5 Canvas, Vanilla ES6+ JS', 'Serves REST endpoints, streams 60fps SSE feeds, plots real-time amount velocity in INR, and provides dual-currency inspection.')
    ]
    for m_title, m_tech, m_desc in modules_data:
        pdf.set_fill_color(241, 245, 249)
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(24, 43, 73)
        pdf.cell(0, 6, m_title + ' (' + m_tech + ')', fill=True, new_x="LMARGIN", new_y="NEXT")
        pdf.set_font('Helvetica', '', 9)
        pdf.set_text_color(40, 40, 40)
        pdf.multi_cell(0, 5, m_desc)
        pdf.ln(2)

    # 6. Anomaly Categories
    pdf.chapter_title('6. Anomaly Categories & Explainability Taxonomy')
    cat_data = [
        ('Amount Deviation', 'Normalized INR amount >= 50,000 INR or exceeds 3-sigma variance.', 'HIGH / CRITICAL', 'Renders INR amount, original currency, conversion rate, and threshold delta.'),
        ('Merchant Outlier', 'Unverified overseas vendor, flagged keywords (UNKNOWN, OVERSEAS).', 'HIGH', 'Highlights suspicious vendor name and international routing tag.'),
        ('Transaction Velocity', 'High burst of successive transactions in a compressed time window.', 'MEDIUM', 'Surfaces velocity counter and rapid-fire transaction sequence.'),
        ('Time-Based Anomaly', 'Transactions occurring during unusual nocturnal hours (e.g., 03:00 AM).', 'MEDIUM', 'Displays event timestamp alongside expected baseline activity curve.'),
        ('Channel Deviation', 'Payment channel mismatch (e.g., high-value POS from international ATM).', 'MEDIUM', 'Contrasts payment channel against profile expectation.'),
        ('Blueprint Seeded Anomaly', 'Pre-configured verification scenario in controlled dataset (Row #8).', 'CRITICAL', 'Fires red visual banner, audio-visual pulse, and model score of 0.98.')
    ]
    for c_name, c_sig, c_sev, c_ui in cat_data:
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_text_color(15, 23, 42)
        pdf.cell(48, 6, c_name, border=1)
        pdf.set_font('Helvetica', '', 8.5)
        pdf.cell(75, 6, c_sig, border=1)
        pdf.set_font('Helvetica', 'B', 8.5)
        pdf.cell(25, 6, c_sev, border=1, align='C')
        pdf.set_font('Helvetica', '', 8.5)
        pdf.multi_cell(42, 6, c_ui, border=1)
    pdf.ln(3)

    # 7. Dashboard Overview
    pdf.chapter_title('7. Dashboard Views & User Interface Features')
    pdf.bullet_point('Diagnostic Telemetry Pills', 'Live indicators for Kafka broker connection, base currency (INR), and ML model status.')
    pdf.bullet_point('Control Toolbar', 'Native file upload with drag-and-drop, 1-click Replay Controlled (10 Txns), Stream Scale (10k Txns), Stop, and Clear.')
    pdf.bullet_point('Playback Pace Selector', 'Allows evaluators to observe streaming at 1x (Realtime), 2x Speed, or 5x (Turbo).')
    pdf.bullet_point('Transaction Velocity Chart', 'Interactive HTML5 Canvas plotting volume in INR with glowing red spikes for detected anomalies.')
    pdf.bullet_point('Dual-Currency Payload Inspector', 'Allows one-click inspection of any stream card, displaying original amount, converted INR value, applied exchange rate, and Kafka offset.')
    pdf.bullet_point('Live Forex Ticker', 'Displays real-time exchange rates for USD, EUR, GBP, AED, SGD, CAD, AUD, and CHF with visual price tick animations.')

    # 8. MVP Delivery
    pdf.chapter_title('8. Minimum Viable Product (MVP) vs Actual Delivery')
    mvp_items = [
        ('Event Ingestion Flow', 'Kafka stream ingestion from synthetic/public data', '100% Delivered (Kafka 7.4.0, Controlled & Scale Replay, CSV Upload)'),
        ('Stream Analytics', 'Spark Structured Streaming windowed metrics', '100% Delivered (Spark Streaming + Python ingestion worker)'),
        ('Forex Normalization', 'Not in initial blueprint (Identified as critical enhancement)', '100% Delivered (Frankfurter API live stream to INR)'),
        ('Anomaly Scoring', 'Isolation Forest feature scoring and alert output', '100% Delivered (Scikit-Learn Isolation Forest v1.0, 5 features, 4 severities)'),
        ('Persistence', 'PostgreSQL database recording transactions and alerts', '100% Delivered (PostgreSQL DDL + zero-config SQLite fallback)'),
        ('Dashboard UI', 'Web interface presenting stream, charts, and alerts', '100% Delivered (Modern glassmorphic UI, Canvas chart, Inspector, Tickers)')
    ]
    for item, req, deliv in mvp_items:
        pdf.set_font('Helvetica', 'B', 8.5)
        pdf.cell(38, 6, item, border=1)
        pdf.set_font('Helvetica', '', 8)
        pdf.cell(65, 6, req, border=1)
        pdf.set_font('Helvetica', 'I', 8)
        pdf.multi_cell(87, 6, deliv, border=1)
    pdf.ln(3)

    # 9. Scope Boundaries
    pdf.chapter_title('9. Scope Boundaries & What Not to Build')
    pdf.body_text('To protect academic feasibility and defend against panel feature-creep, the following boundaries remain strictly enforced:')
    pdf.bullet_point('No Real Banking Gateways', 'Synthetic and public transaction datasets are used exclusively; live credit card networks are out of scope.')
    pdf.bullet_point('No Automated Fund Freezing', 'The system generates explainable alerts for human reviewers; it does not execute automated account blocks.')
    pdf.bullet_point('No Complex Model Ensembles', 'Isolation Forest provides clean, defensible unsupervised anomaly detection without unexplainable black-box complexity.')

    # Output path
    output_pdf = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'docs', 'FinX_Updated_Major_Project_Blueprint.pdf')
    pdf.output(output_pdf)
    print(f'Successfully generated PDF at: {output_pdf}')

if __name__ == '__main__':
    generate_pdf()
