import markdown
import os

# Lire le fichier markdown
with open('bilan_projet_dataflow.md', 'r', encoding='utf-8') as f:
    md_content = f.read()

# Convertir en HTML avec le support des tables
html_content = markdown.markdown(md_content, extensions=['tables', 'fenced_code'])

# Template HTML avec CSS pour un rendu professionnel
html_template = f"""
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <title>Bilan de Projet - Orange Digital Center</title>
    <style>
        @page {{
            size: A4;
            margin: 2.5cm;
        }}
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
        }}
        h1 {{
            color: #FF6600;
            border-bottom: 3px solid #FF6600;
            padding-bottom: 10px;
            text-align: center;
        }}
        h2 {{
            color: #FF6600;
            border-bottom: 2px solid #FF6600;
            padding-bottom: 5px;
            margin-top: 30px;
        }}
        h3 {{
            color: #333;
            margin-top: 20px;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 15px 0;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 10px;
            text-align: left;
        }}
        th {{
            background-color: #FF6600;
            color: white;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        code {{
            background-color: #f4f4f4;
            padding: 2px 6px;
            border-radius: 3px;
            font-family: monospace;
        }}
        ul {{
            margin-left: 20px;
        }}
        li {{
            margin: 5px 0;
        }}
        hr {{
            border: none;
            border-top: 1px solid #ddd;
            margin: 20px 0;
        }}
        strong {{
            color: #FF6600;
        }}
    </style>
</head>
<body>
{html_content}
</body>
</html>
"""

# Sauvegarder le HTML
with open('bilan_projet_dataflow.html', 'w', encoding='utf-8') as f:
    f.write(html_template)

print("✅ Fichier HTML généré : bilan_projet_dataflow.html")
print("📌 Pour convertir en PDF, ouvrez le fichier HTML dans votre navigateur et utilisez Ctrl+P > Enregistrer en PDF")
