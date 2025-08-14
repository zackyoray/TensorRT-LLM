# Mermaid Syntax Validation Guide for Platform Administration

This guide provides **correct syntax patterns** and **common pitfalls** for Mermaid diagrams used in OpenShift platform documentation.

## 🧪 **Quick Validation Method**

Test each diagram individually before adding to documents:

```bash
# Test single diagram
echo 'graph TD; A-->B;' > test.mmd && mmdc -i test.mmd -o test.png

# Test from file
mmdc -i your-diagram.mmd -o validation.png
```

---

## 1. **Flowcharts** - Architecture & Process Flows

### ✅ **Correct Syntax**

```mermaid
flowchart TD
    subgraph "Control Plane"
        API[API Server]
        ETCD[(etcd Cluster)]
        SCHED[Scheduler]
    end
    
    subgraph "Worker Nodes"
        NODE1[Node 1]
        NODE2[Node 2]
    end
    
    API --> ETCD
    API --> SCHED
    SCHED --> NODE1
    SCHED --> NODE2
    
    style API fill:#ee0000,color:#fff
    style ETCD fill:#1168bd,color:#fff
```

### ❌ **Common Mistakes**

```mermaid
# WRONG: Missing direction
graph
    A --> B

# WRONG: Invalid subgraph syntax
subgraph Control Plane  # Missing quotes
    API[API Server]

# WRONG: Invalid style syntax
style API fill:red      # Missing quotes and #
```

### 🎯 **OpenShift Examples**

```mermaid
flowchart TB
    subgraph "OpenShift Cluster"
        API[API Server<br/>Port 6443]
        ETCD[(etcd<br/>Port 2379)]
        SCHED[Scheduler]
        CM[Controller Manager]
    end
    
    subgraph "Infrastructure"
        LB[Load Balancer<br/>HAProxy]
        ROUTER[OpenShift Router]
        REGISTRY[Internal Registry]
    end
    
    subgraph "Compute Nodes"
        N1[Worker Node 1<br/>kubelet + CRI-O]
        N2[Worker Node 2<br/>kubelet + CRI-O]
        N3[GPU Node<br/>NVIDIA Driver]
    end
    
    LB --> API
    API --> ETCD
    API --> SCHED
    API --> CM
    SCHED --> N1
    SCHED --> N2
    SCHED --> N3
    
    style API fill:#ee0000,color:#fff
    style N3 fill:#76b900,color:#fff
```

---

## 2. **Sequence Diagrams** - Incident Response & API Flows

### ✅ **Correct Syntax**

```mermaid
sequenceDiagram
    participant User as End User
    participant Monitor as Monitoring
    participant OnCall as On-Call Engineer
    participant IC as Incident Commander
    
    User->>Monitor: Issue Detected
    Monitor->>OnCall: PagerDuty Alert
    
    Note over OnCall: Assessment<br/>15 min SLA
    
    OnCall->>IC: Escalate to P1
    
    alt Critical Issue
        IC->>OnCall: Immediate Response
        OnCall->>Monitor: Mitigation Applied
    else
        Note over IC: Standard Process
        IC->>OnCall: Follow Runbook
    end
    
    loop Status Updates
        OnCall->>IC: Progress Update
        IC->>User: Status Communication
    end
    
    OnCall->>IC: Issue Resolved
    IC->>User: All Clear
```

### ❌ **Common Mistakes**

```mermaid
# WRONG: Text after else
else Critical Issue        # Should be just "else"

# WRONG: Missing participant declaration
User->>Monitor: Message    # User not declared

# WRONG: Invalid arrow syntax
User-->Monitor: Message    # Should be ->> for sequence

# WRONG: Break outside alt/loop
break                      # Must be inside alt block
```

### 🎯 **Key Sequence Rules**

1. **Participants**: Always declare at the top
2. **Arrows**: Use `->>` for messages, `-->>` for responses
3. **Alt blocks**: `else` is standalone, no text
4. **Notes**: `Note over` or `Note left of` / `Note right of`
5. **Loops**: Must have proper `end`
6. **Break**: Only inside `alt` blocks

---

## 3. **State Diagrams** - System Status & Workflows

### ✅ **Correct Syntax**

```mermaid
stateDiagram-v2
    [*] --> Initializing
    
    Initializing --> Healthy: Startup Complete
    Healthy --> Warning: Degraded Performance
    Warning --> Critical: Service Outage
    Critical --> Warning: Partial Recovery
    Warning --> Healthy: Full Recovery
    
    Healthy --> Maintenance: Planned Downtime
    Maintenance --> Healthy: Maintenance Complete
    
    Critical --> [*]: System Shutdown
    
    state Healthy {
        [*] --> Running
        Running --> Monitoring
        Monitoring --> Running
    }
    
    state Critical {
        [*] --> Alerting
        Alerting --> Escalating
        Escalating --> Resolving
        Resolving --> [*]
    }
```

### ❌ **Common Mistakes**

```mermaid
# WRONG: Using old state syntax
state A {
    state B
}
# Should use stateDiagram-v2

# WRONG: Missing arrow
Healthy Warning            # Should be: Healthy --> Warning

# WRONG: Invalid nested state
state Healthy
    Running --> Monitoring # Should be inside state block
```

---

## 4. **Gantt Charts** - Project Timelines

### ✅ **Correct Syntax**

```mermaid
gantt
    title OpenShift Migration Timeline
    dateFormat YYYY-MM-DD
    axisFormat %m/%d
    
    section Planning
    Requirements     :req, 2024-01-01, 2024-01-15
    Architecture     :arch, after req, 10d
    Hardware Proc    :hw, 2024-01-10, 2024-02-15
    
    section Implementation
    Cluster Setup    :setup, after hw, 7d
    Node Config      :nodes, after setup, 5d
    Application Mig  :apps, after nodes, 10d
    
    section Validation
    Testing          :test, after apps, 5d
    Go-Live         :golive, after test, 1d
```

### ❌ **Common Mistakes**

```mermaid
# WRONG: Missing dateFormat
gantt
    title Project Timeline  # Missing dateFormat YYYY-MM-DD

# WRONG: Invalid date dependency
Task1 :after task2, 5d     # Should be: Task1 :task1, after task2, 5d

# WRONG: Missing section
Task1 :task1, 2024-01-01, 5d  # Should be under a section
```

---

## 5. **Journey Maps** - User/Process Flows

### ✅ **Correct Syntax**

```mermaid
journey
    title Developer Onboarding Journey
    section Account Setup
      Create Red Hat ID    : 3: Developer
      Request Access       : 2: Developer, Manager  
      Receive Credentials  : 4: Developer, IT Admin
      
    section First Deployment
      Install CLI Tools    : 3: Developer
      Create Project       : 2: Developer, Platform Team
      Deploy Application   : 4: Developer, DevOps
      
    section Production Ready
      Setup Monitoring     : 3: Developer, SRE
      Configure Scaling    : 4: Developer, Platform Team
      Go Live             : 5: Developer, Manager
```

### ❌ **Common Mistakes**

```mermaid
# WRONG: Missing score
Create Account: Developer              # Missing score (1-5)

# WRONG: Invalid section syntax
section: Account Setup                 # Should be: section Account Setup

# WRONG: Invalid actor format
Create Account: 3: Developer Manager   # Should be: 3: Developer, Manager
```

---

## 6. **C4 Context/Container Diagrams** - Architecture

### ✅ **Correct Syntax**

```mermaid
C4Context
    title OpenShift Platform Context
    
    Person(dev, "Developer", "Application Developer")
    Person(ops, "Platform Admin", "OpenShift Administrator")
    
    System(openshift, "OpenShift Platform", "Container orchestration")
    System_Ext(monitoring, "Monitoring", "Prometheus/Grafana")
    System_Ext(registry, "Image Registry", "Quay.io")
    
    Rel(dev, openshift, "Deploys apps")
    Rel(ops, openshift, "Manages platform")
    Rel(openshift, monitoring, "Sends metrics")
    Rel(openshift, registry, "Pulls images")
```

### ❌ **Common Mistakes**

```mermaid
# WRONG: Missing C4Context declaration
Person(dev, "Developer")              # Should start with C4Context

# WRONG: Invalid relationship syntax
Rel(dev, openshift)                   # Missing description

# WRONG: Wrong element type
SystemExt(monitoring, "External")     # Should be System_Ext
```

---

## 7. **Git Graphs** - Release & Branching

### ✅ **Correct Syntax**

```mermaid
gitgraph
    commit id: "Initial"
    branch develop
    checkout develop
    commit id: "Add user authentication"
    commit id: "Implement data validation"
    
    checkout main
    branch hotfix
    checkout hotfix
    commit id: "Critical Fix"
    
    checkout main
    merge hotfix
    tag: "v1.0.1"
    
    checkout develop
    merge main
    commit id: "Feature 3"
    
    checkout main
    merge develop
    tag: "v1.1.0"
```

### ❌ **Common Mistakes**

```mermaid
# WRONG: Invalid branch syntax
branch: develop                       # Should be: branch develop

# WRONG: Merge without checkout
merge develop                         # Should checkout target first

# WRONG: Invalid tag syntax
tag "v1.0"                           # Should be: tag: "v1.0"
```

---

## 8. **Quadrant Charts** - Analysis & Planning

### ✅ **Correct Syntax**

```mermaid
quadrantChart
    title Resource Utilization Analysis
    x-axis Low Usage --> High Usage
    y-axis Low Priority --> High Priority
    
    quadrant-1 Scale Up
    quadrant-2 Monitor  
    quadrant-3 Decommission
    quadrant-4 Optimize
    
    "Production API": [0.8, 0.9]
    "ML Training": [0.9, 0.8]
    "Dev Env": [0.3, 0.4]
    "Legacy App": [0.2, 0.2]
```

### ❌ **Common Mistakes**

```mermaid
# WRONG: Missing axis labels
quadrantChart
    title Analysis               # Missing x-axis and y-axis

# WRONG: Invalid quadrant syntax
quadrant1 Scale Up              # Should be: quadrant-1 Scale Up

# WRONG: Invalid data point format
"Production": 0.8, 0.9          # Should be: "Production": [0.8, 0.9]
```

---

## 🚀 **Testing Your Diagrams**

### **Individual Testing Script**

```bash
#!/bin/bash
# test-diagram.sh

echo "Testing diagram syntax..."

# Create test file
cat > test-diagram.mmd << 'EOF'
# Paste your diagram here
EOF

# Test with mmdc
if mmdc -i test-diagram.mmd -o test-output.png; then
    echo "✅ Diagram syntax is valid!"
    echo "📁 Output: test-output.png"
else
    echo "❌ Syntax error detected!"
    echo "🔧 Check the error message above"
fi

# Cleanup
rm -f test-diagram.mmd test-output.png
```

### **Batch Testing All Diagrams**

```bash
# Extract and test all diagrams from markdown
grep -n '```mermaid' your-doc.md | while read line; do
    line_num=$(echo $line | cut -d: -f1)
    echo "Testing diagram at line $line_num..."
    
    # Extract diagram content and test
    # (Implementation would extract between ```mermaid and ```)
done
```

---

## 🎯 **Platform Admin Quick Reference**

### **Most Common Diagram Types:**

1. **Flowcharts**: Architecture, process flows, troubleshooting
2. **Sequence**: Incident response, API interactions, deployments  
3. **State**: System health, deployment states, workflow status
4. **Gantt**: Migration timelines, maintenance windows, projects
5. **Journey**: User onboarding, developer experience, workflows

### **Validation Checklist:**

- [ ] Proper diagram type declaration
- [ ] All elements properly defined
- [ ] Correct arrow syntax for diagram type
- [ ] Valid color codes (hex with #)
- [ ] Proper subgraph/section syntax
- [ ] No trailing spaces or special characters
- [ ] Consistent indentation
- [ ] All blocks properly closed (end statements)

### **Quick Syntax Test:**

```bash
# Add this to your workflow
./convert.sh --test-only your-document.md
# (This would run syntax validation without full conversion)
```

Now you have a comprehensive syntax guide to create bulletproof Mermaid diagrams for your OpenShift documentation! 🎯

🦅🦅🦅 