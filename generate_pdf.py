from fpdf import FPDF
import os

pdf = FPDF()
pdf.add_page()
pdf.set_font("Helvetica", size=12)

# Title
pdf.set_font("Helvetica", style="B", size=16)
pdf.cell(200, 10, txt="FinX: Data Ingestion (Module 1) Demo Procedure", ln=True, align='C')
pdf.ln(10)

content = """
Here is your exact checklist to start everything from a completely closed, blank state.

Step 1: Wake up Docker (Crucial First Step)
Since you closed everything, your background services (like Kafka) are turned off. 
1. Open the Docker Desktop application on your computer.
2. Wait a minute for the icon in the bottom-left corner to turn green and say "Engine running". 
   (Do not skip this, or the script will crash with a Timeout error).

Step 2: Open your Project
1. Open your terminal or VS Code.
2. Make sure you are in your project folder (FinX).

Step 3: Turn on the Streaming Infrastructure
1. In your terminal, start the Kafka cluster by running:
   docker-compose up -d
2. Wait a few seconds for it to say 'Started' for both zookeeper and kafka.

Step 4: Run the Demonstration
Now your system is ready for the presentation.

To demonstrate the "Huge" Dataset (the fast stream):
   python streaming/dataset_replay.py --dataset huge
(Press Ctrl + C when you are ready to stop it).

To demonstrate the "Controlled" Dataset (slow stream with seeded anomaly):
   python streaming/dataset_replay.py --dataset controlled

Step 5: Shutting Down (After the demo)
When you are completely finished with your presentation and want to close everything down cleanly, run:
   docker-compose down
"""

pdf.set_font("Helvetica", size=11)
for line in content.split('\n'):
    pdf.cell(200, 7, txt=line, ln=True)

script_dir = os.path.dirname(os.path.abspath(__file__))
output_path = os.path.join(script_dir, "docs", "Demo_Procedure_Guide.pdf")

pdf.output(output_path)
print("PDF Generated Successfully")
