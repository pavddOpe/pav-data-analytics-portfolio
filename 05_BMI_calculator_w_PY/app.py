import gradio as gr

def calculate_bmi(name, weight, height):
    if weight <= 0 or height <= 0:
        return "❌ Invalid measurements. Please provide valid measurements."
    
    bmi = weight / (height ** 2)
    bmi = round(bmi, 2)
    
    if bmi < 18.5:
        status = "Underweight"
    elif bmi < 25:
        status = "Normal weight"
    elif bmi < 30:
        status = "Overweight"
    else:
        status = "Obese" if bmi < 35 else "Severely obese" if bmi < 40 else "Morbidly obese"
    
    return f"""
### ✅ Result
**Name:** {name or 'Person'}  
**BMI:** `{bmi}`  
**Category:** **{status}**
"""


css = """
body {
    background-color: #1a1d21;
    color: #e0e0e5;
    font-family: system-ui, -apple-system, sans-serif;
}
.gradio-container {
    max-width: 460px !important;
    margin: 60px auto !important;
    padding: 0 16px;
}
.main-card {
    background: #24272c;
    padding: 32px;
    border-radius: 16px;
    box-shadow: 0 10px 30px rgba(0,0,0,0.4);
    border: 1px solid #33363d;
}
label {
    color: #c0c4cc !important;
    font-weight: 500;
}
input, textarea, select {
    background-color: #2e3238 !important;
    color: #e0e0e5 !important;
    border: 1px solid #44484f !important;
    border-radius: 8px;
}
input::placeholder {
    color: #6b7280 !important;
}
button {
    border-radius: 8px !important;
    font-weight: 600;
    transition: all 0.2s;
}
.primary-button {
    background: #3b82f6 !important;
    color: white !important;
}
.primary-button:hover {
    background: #2563eb !important;
}
.secondary-button {
    background: #4b5563 !important;
    color: #e0e0e5 !important;
}
.secondary-button:hover {
    background: #6b7280 !important;
}
.markdown {
    color: #e0e0e5 !important;
}
.markdown h3 {
    color: #60a5fa !important;
}
.success-text {
    color: #34d399 !important;
}
"""

with gr.Blocks(css=css, title="BMI Calculator") as demo:
    with gr.Column(elem_classes="main-card"):
        gr.Markdown("<h2 style='text-align:center; margin-bottom: 24px;'>BMI Calculator</h2>")
        
        name = gr.Textbox(
            label="Name",
            placeholder="Enter your name (optional)",
            elem_classes="input-field"
        )
        weight = gr.Number(
            label="Weight (kg)",
            placeholder="Enter your weight in kgs",
            elem_classes="input-field"
        )
        height = gr.Number(
            label="Height (m)",
            placeholder="Enter your height in meters (ex. 1.80)",
            elem_classes="input-field"
        )
        
        with gr.Row(equal_height=True):
            clear_btn = gr.Button("Clear", variant="secondary", elem_classes="secondary-button")
            submit_btn = gr.Button("Calculate", variant="primary", elem_classes="primary-button")
        
        output = gr.Markdown()

    submit_btn.click(
        fn=calculate_bmi,
        inputs=[name, weight, height],
        outputs=output
    )
    clear_btn.click(
        fn=lambda: ("", "", "", ""),
        outputs=[name, weight, height, output]
    )

demo.launch(server_name="0.0.0.0", server_port=7860)