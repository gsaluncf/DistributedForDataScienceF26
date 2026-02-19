# Git Lab Walkthrough Scripts

## Purpose
These scripts help validate the Git lab workflow and demonstrate Git internals for Week 1.

## Files
1. **`git_lab_setup_env.bat`** - One-time environment setup
2. **`git_lab_walkthrough.bat`** - Interactive walkthrough with pauses

## How to Use

### First Time Setup
1. Open Command Prompt (cmd.exe) - **NOT PowerShell**
2. Navigate to this directory
3. Run: `git_lab_setup_env.bat`
4. Follow prompts to configure Git identity

### Run the Walkthrough
1. Open Command Prompt (cmd.exe)
2. Navigate to this directory
3. Run: `git_lab_walkthrough.bat`
4. Press SPACE at each pause to continue
5. Observe both raw `.git` files AND git commands

## What the Walkthrough Does

### Step 1: Initialize repo + first commit
- Creates `git_lab_repo/` directory
- Initializes Git repository
- Creates first file and commits
- **Shows**: `.git/HEAD`, `.git/refs/heads/main`, `git log`

### Step 2: Create feature branch + commit
- Creates `feature1` branch
- Makes a change and commits
- **Shows**: Branch pointers are different hashes (branches = sticky notes!)

### Step 3: Commit on main + merge (with conflict)
- Switches to main
- Makes conflicting change on same line
- Attempts merge → **CONFLICT!**
- Shows conflict markers
- Resolves and completes merge
- **Shows**: Merge commit with two parents (the DAG)

### Step 4: Inspect Git objects
- Uses `git cat-file` to inspect:
  - Commit object (metadata + pointers)
  - Tree object (directory listing)
  - Blob object (file content)
- **Proves**: Git is a database of snapshots, not diffs

### Step 5: Reflog - your safety net
- Shows `git reflog` (every move HEAD made)
- Shows raw `.git/logs/HEAD` file
- **Proves**: Git rarely deletes anything; reflog is recovery tool

## Expected Output
Each step shows:
1. **Action taken** (what the script did)
2. **Evidence - Git Commands** (`git status`, `git log --graph`, etc.)
3. **Evidence - Raw Files** (`.git/HEAD`, `.git/refs/heads/*`, etc.)
4. **Auto-checks** (✓ On correct branch, ✓ Working tree clean)
5. **Pause** with explanation of what to observe

## Troubleshooting

### "Git not found in PATH"
Install Git for Windows: https://git-scm.com/download/win

### "git commit failed"
Run `git_lab_setup_env.bat` first to configure identity

### Script won't run
Make sure you're using **cmd.exe**, not PowerShell

### Want to run again
Delete the `git_lab_repo/` folder or let the script delete it

## Next Steps
Once validated, these concepts will be turned into:
- Canvas HTML page (following style-guide.txt)
- In-class walkthrough material
- Student lab instructions

