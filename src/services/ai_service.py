import os
import json
import re
from typing import List, Optional
try:
    from cerebras.cloud.sdk import Cerebras
except ImportError:
    Cerebras = None

async def generate_project_details(title: str, description: str, language: Optional[str], topics: List[str]) -> Optional[dict]:
    """
    Calls Cerebras (Llama3.1-70b) to generate structured project details for a GitHub repository.
    Returns a dictionary with case_study, problem_statement, solution_description, prerequisites, and deliverables.
    """
    api_key = os.getenv("CEREBRAS_API_KEY")
    if not api_key:
        print("Warning: CEREBRAS_API_KEY is not set. Skipping AI generation.")
        return None

    if not Cerebras:
        print("Warning: cerebras_cloud_sdk is not installed. Run `uv add cerebras_cloud_sdk`.")
        return None

    client = Cerebras(api_key=api_key)
    
    prompt = f"""
    You are an expert technical writer and curriculum developer for Project Hub.
    I have a GitHub repository that I want to turn into a comprehensive project tutorial.
    Please generate detailed, realistic, and professional educational content based on the following metadata:
    
    Project Title: {title}
    Description: {description or 'No description provided.'}
    Language: {language or 'Various'}
    Topics/Tech Stack: {', '.join(topics) if topics else 'General Programming'}
    
    Generate the response as a JSON object that strictly matches the following schema:
    {{
        "case_study": "A 2-3 sentence paragraph providing real-world context on why this project exists and the business need it solves.",
        "problem_statement": "A 2-3 sentence paragraph explaining the specific technical or operational problem this project addresses.",
        "solution_description": "A 2-3 sentence paragraph describing how this project works, mentioning the tech stack and key features.",
        "prerequisites": ["A list of 3-5 strings detailing knowledge required before starting (e.g. 'Basic understanding of REST APIs', 'Familiarity with React')."],
        "deliverables": ["A list of 4-6 strings detailing what the user will build/deploy (e.g. 'Fully functional backend', 'Interactive dashboard')."],
        "sub_domain": "A specific sub-category string like 'Web Development', 'Machine Learning', 'Data Engineering', 'Mobile App', etc.",
        "difficulty": "One of: 'EASY', 'MEDIUM', 'ADVANCED', 'EXPERT'",
        "estimated_min_time": 10,  # Minimum hours to complete (integer)
        "estimated_max_time": 40,  # Maximum hours to complete (integer)
        "language": "Primary programming language to use (e.g. 'Python', 'TypeScript', 'Rust')"
    }}
    
    Output ONLY valid JSON. Do not include markdown code blocks, backticks, or any other text before or after the JSON.
    """

    try:
        try:
            # Primary model attempt
            response = client.chat.completions.create(
                messages=[
                    {"role": "system", "content": "You are a specialized JSON data generator. Only output raw valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                model="gpt-oss-120b",
            )
        except Exception as e:
            print(f"Cerebras primary model (gpt-oss-120b) failed: {e}. Falling back to llama3.1-8b...")
            # Fallback model attempt
            response = client.chat.completions.create(
                messages=[
                    {"role": "system", "content": "You are a specialized JSON data generator. Only output raw valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                model="llama3.1-8b",
            )
        
        reply = response.choices[0].message.content.strip()
        
        # Strip potential markdown formatting if the model disobeys instructions
        if reply.startswith("```"):
            reply = re.sub(r"^```[a-zA-Z]*\n|```$", "", reply, flags=re.MULTILINE).strip()
            
        return json.loads(reply)
        
    except Exception as e:
        print(f"Cerebras API Error (Both models failed): {e}")
        return None
