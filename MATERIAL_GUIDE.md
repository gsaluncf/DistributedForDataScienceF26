# Handoff: Creating Course Materials for Distributed Computing for DS

This guide outlines the standards and workflow for creating Week-level materials (Lectures, Labs, and Notebooks) in this repository.

## 0. Repository & Deployment Architecture (The "Public Proxy")

This repository (`weeks/` locally, `gsaluncf/publicfiles` on GitHub) is a multi-purpose distribution hub.

*   **Public URL**: Files in `weeks/weekXX/` are served at `https://ncfdatascience.com/weekXX/`.
*   **Proxy Nature**: These files are often embedded into Canvas via iframes.
*   **Asset Pathing**: Images and assets should ideally use absolute URLs (e.g., `https://ncfdatascience.com/weekXX/images/diagram.png`) to ensure they render correctly within Canvas across different domains.
*   **Scope**: This repo "serves many masters." Only modify files within the specific week folder assigned to your task.

## 1. Lecture Files (`lecture_N.html`)
Lectures are rich, interactive HTML documents designed to be hosted on GitHub Pages and embedded in Canvas.

*   **Aesthetics**: Use a modern, premium look. Avoid plain browser defaults.
    *   **Colors**: Use HSL-tailored colors (e.g., `#1e40af` blues, `#065f46` greens).
    *   **Layout**: Use 8px border-radius containers, left-border accents for headers, and clean tables with sticky-style headers.
*   **Visuals**: Use **Embedded SVGs** for diagrams (architecture, scaling, partitioning). No external image dependencies or placeholders.
*   **Interaction**: Break content into "Parts" (Part 1, 2, etc.) using `<h2>` tags. Include "Check your understanding" boxes at the end of each major conceptual section.
*   **Comparison Tables**: Always provide a comparison between SQL (what they know) and the Distributed/Cloud system (what is new).

## 2. Lab Files (`lab_N.html`)
Labs provide the high-level roadmap and setup instructions before the student jumps into the code.

*   **Credential Setup**: This is the most critical section. Instructions must be specific to the current local terminal environment (e.g., `cat ~/.aws/credentials`) and the target execution environment (e.g., Colab Secrets). Keep it updated; remove outdated references like "AWS Academy".
*   **Open in Colab**: Provide a prominent "Open in Google Colab" button that links to the specific `.ipynb` file in the public repository.
*   **Checklist**: Include a "What You Will Do" table and a "Cleanup Checklist" to ensure students don't leave expensive resources running.

## 3. Hands-on Notebooks (`.ipynb`)
Notebooks are the primary learning environment.

*   **Setup Cell**: Must include logic to load credentials from **Colab Secrets** while gracefully falling back for local execution.
*   **Step-by-Step Flow**:
    1.  **Creation**: Define resources (e.g., Table, Queue).
    2.  **CRUD**: Demonstrate the primary API calls (using `boto3`).
    3.  **Modern Path**: Include modern querying paths (e.g., PartiQL) and Data Science integrations (e.g., loading results directly into Pandas DataFrames).
    4.  **Mini Challenge**: End with a task where students must apply what they learned by adding their own data or logic.
    5.  **Cleanup**: Always provide a final cell that deletes the created resources.

## Key Technical Patterns
*   **Boto3 SDK**: Prefer the high-level `resource` interface for simple CRUD but use the `client` interface for advanced features like PartiQL (`execute_statement`).
*   **Decimals**: Explicitly teach handling `Decimal` vs `float` in DynamoDB contexts.
*   **Horizontal Scaling**: Always emphasize how the system scales (e.g., partition splitting) versus specialized vertical scaling in traditional SQL.
