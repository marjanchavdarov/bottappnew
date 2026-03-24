name: MCP Price Sync

on:
  schedule:
    # Run daily at 2:00 AM
    - cron: '0 2 * * *'
  workflow_dispatch:  # Allow manual trigger
    inputs:
      sync_type:
        description: 'Sync type (all or specific)'
        required: false
        default: 'all'
        type: choice
        options:
          - all
          - specific

jobs:
  sync:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout code
      uses: actions/checkout@v4
    
    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: '3.10'
        cache: 'pip'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
    
    - name: Run MCP sync
      env:
        SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
        SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
      run: |
        python mcp_ingest.py
    
    - name: Upload logs on failure
      if: failure()
      uses: actions/upload-artifact@v4
      with:
        name: sync-logs
        path: |
          *.log
          mcp_ingest.log
        retention-days: 7
