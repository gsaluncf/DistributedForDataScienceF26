# Week 1 Student Analysis

## Student Perspective: Working Through Week 1

### Initial Setup
- Created this directory to track my work and observations as a student
- Will document confusing terms, unclear instructions, and actual work done

### Terms That May Be Confusing to Students
- **Fast-forward merge** vs **merge commit** - not clearly explained upfront
- **Upstream** - what does this mean exactly?
- **Conflict markers** - what do they look like?
- **IAM** - acronym not spelled out initially
- **Access keys** vs **credentials** - used interchangeably
- **CLI profiles** - concept not well explained
- **Least privilege** - principle mentioned but not defined
- **Account ID** - why redact it? Security implications unclear

### Instruction Issues/Confusion
1. **GitHub Classroom link** - mentioned but not provided (phantom step)
2. **Conflicting change from upstream** - how do I know what this is?
3. **CLI installation** - implied but not explicitly linked
4. **AWS account creation** - free tier vs credits distinction unclear
5. **Budget setup** - mentioned but detailed steps missing

### Missing Prerequisites
- Git installation steps
- Python 3.10+ requirement mentioned in notes but not in main instructions
- AWS CLI v2 installation guide

### Work Log - Part A: Git Conflict Kata

**Immediate Issues Encountered:**
1. **No GitHub Classroom link provided** - Instructions say "link in Canvas" but I don't have access
2. **PowerShell command issues** - Git commands with quotes don't work as expected in PowerShell
3. **Missing setup steps** - No guidance on git config setup for first-time users

**Student Confusion Points:**
- Step 4: "apply the instructor's conflicting change from upstream (already pushed)" - How do I know what this change is?
- "indicated lines in /bio/README.md" - Which lines exactly?
- What does a conflict marker look like? No example provided
- "CI (autograding)" - What is CI? Not explained

**Attempted Workaround:**
- Created mock repository structure to simulate the exercise
- Hit PowerShell command parsing issues immediately
- Would need to use Git Bash or modify commands for PowerShell

**Missing Prerequisites for Students:**
- Git configuration (user.name, user.email)
- Understanding of PowerShell vs Bash command differences on Windows
- Example of what conflict markers look like

### Work Log - Part B: GitHub Pages

**Created:** Simple HTML file in docs/ folder
**Issues:**
- Instructions assume students know HTML basics
- No template provided for the index.html file
- "with your name and course" - minimal guidance on structure

### Work Log - Part C: AWS CLI Check

**Major Issue:** AWS CLI not installed!
- Command `aws --version` fails with "command not found"
- Instructions mention CLI installation is "implied rather than linked"
- Students would be completely stuck here without installation guide

**Student would need:**
1. AWS CLI installation instructions
2. AWS account creation guide
3. IAM user setup walkthrough
4. CLI configuration steps

### Overall Week 1 Assessment
**Completion Status:** Cannot complete without external help
**Biggest Blockers:**
1. Missing GitHub Classroom link
2. AWS CLI not installed
3. Unclear conflict resolution steps
4. PowerShell command compatibility issues
