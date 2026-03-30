# Azure Function App Migration Guide: EP1 → Flexible Consumption Plan

> Implementation guide for migrating an Azure Function App from Elastic Premium (EP1) to Flexible Consumption plan. This document is intended for a cloud engineer to follow step-by-step.

---

## Table of Contents

1. [Security Audit Checklist](#1-security-audit-checklist)
2. [Flex Consumption Constraints](#2-flex-consumption-plan--key-constraints-to-validate)
3. [Phase 1 — Complete Security Audit](#phase-1-complete-security-audit-current-function-app)
4. [Phase 2 — Pre-Migration Preparation](#phase-2-pre-migration-preparation) (includes subnet creation via Portal)
5. [Phase 3 — Provision New Function App](#phase-3-provision-new-function-app)
6. [Phase 4 — Deploy & Validate](#phase-4-deploy--validate)
7. [Phase 5 — Cutover & Decommission](#phase-5-cutover--decommission)
8. [Azure CLI Audit Commands](#6-recommended-cli-commands-for-audit)
9. [Resource Reuse Reference](#7-resource-reuse-reference)

---

## 1. Security Audit Checklist

The following checklist covers all areas to audit on the **current EP1 function app** before migration. Items marked 🔴 are critical, 🟠 important, 🟡 nice-to-have.

---

### 🔴 Critical — Must Audit

#### Inbound Connection (already in scope)
- [ ] Public network access — disabled?
- [ ] Access restrictions — list all rules
- [ ] Private endpoint — document PE resource, subnet, IP
- [ ] Private DNS — `privatelink.azurewebsites.net` zone and VNet link

#### Outbound Connection (already in scope)
- [ ] VNet integration enabled?
- [ ] Route all enabled?
- [ ] User-defined routes (UDR) — document route table and rules
- [ ] NAT gateway — document if attached

#### Identity & Access Management
- [ ] Managed Identity — system-assigned enabled? User-assigned identities listed?
- [ ] RBAC Role Assignments — who/what has Contributor, Reader, or custom roles on the function app
- [ ] Function-level Auth Keys — host keys, function keys, master key — how are these managed?
- [ ] EasyAuth (Authentication/Authorization) — is built-in auth enabled? Which identity provider?

#### Secrets & Configuration
- [ ] App Settings audit — list all setting names (not values), flag which contain secrets
- [ ] Key Vault References — are secrets sourced via `@Microsoft.KeyVault(...)` syntax?
- [ ] Key Vault Access — does the managed identity have correct Key Vault permissions?
- [ ] Identify plain-text secrets that should be migrated to Key Vault

#### TLS / Transport Security
- [ ] Minimum TLS Version — must be 1.2
- [ ] HTTPS Only — is HTTP-to-HTTPS redirect enforced?
- [ ] Client Certificate Mode — is mutual TLS required?
- [ ] Custom Domains & SSL Bindings — list any custom domains

---

### 🟠 Important — Should Audit

#### Storage Security
- [ ] List all storage accounts used by the function app (including Durable Functions)
- [ ] Storage Account Firewall — locked to VNet/private endpoints or open to all networks?
- [ ] Storage Private Endpoints — separate PEs for blob, table, queue, file shares?
- [ ] Storage Auth Method — shared access keys vs Managed Identity for `AzureWebJobsStorage`?
- [ ] Storage Encryption — customer-managed keys (CMK) or Microsoft-managed?
- [ ] Durable Functions Task Hub — document task hub name, storage account, partition count

#### Network Security
- [ ] NSG Rules — capture rules on VNet integration subnet and private endpoint subnet
- [ ] Service Endpoints — any configured on subnets?
- [ ] DNS Resolution — custom DNS servers on VNet or Azure-provided DNS?
- [ ] Firewall/NVA — is traffic routed through Azure Firewall or Network Virtual Appliance?

#### Monitoring & Compliance
- [ ] Diagnostic Settings — are platform logs sent to Log Analytics / Storage / Event Hub?
- [ ] Application Insights — connection string; sampling rate; private link scope?
- [ ] Microsoft Defender for Cloud — any active recommendations?
- [ ] Azure Policy — any policies enforcing security baselines?

#### Application Configuration
- [ ] CORS Settings — list allowed origins; is wildcard `*` used?
- [ ] IP Restrictions on SCM/Kudu — separately restricted?
- [ ] Remote Debugging — must be disabled in production
- [ ] FTP State — must be `Disabled` or `FtpsOnly`
- [ ] Runtime Version & Python Version — document current versions

---

### 🟡 Nice-to-Have — Optional Audit

#### Operational
- [ ] Deployment Slots — any staging slots with slot-specific settings?
- [ ] Deployment Method — ZIP deploy, GitHub Actions, Azure DevOps?
- [ ] Scaling Configuration — EP1 pre-warmed instance count
- [ ] Function Bindings Inventory — full list of triggers and bindings

#### Governance
- [ ] Resource Locks — delete or read-only locks?
- [ ] Tags — environment, cost-center, owner tags?
- [ ] Subscription/Resource Group Permissions — broader RBAC?

---

## 2. Flex Consumption Plan — Key Constraints to Validate

> [!IMPORTANT]
> Flex Consumption is still evolving. Validate these constraints against [current Azure documentation](https://learn.microsoft.com/en-us/azure/azure-functions/flex-consumption-plan) before proceeding.

| Capability | EP1 (Current) | Flex Consumption | Action Required |
|---|---|---|---|
| VNet Integration | ✅ Full support | ✅ Supported (built-in) | Verify subnet sizing — Flex uses `/24` minimum |
| Subnet Delegation | `Microsoft.Web/serverFarms` | `Microsoft.App/environments` | **New subnet required** — cannot reuse EP1 subnet |
| Private Endpoints (Inbound) | ✅ Supported | ⚠️ Check GA status | Confirm availability in your region |
| Always-Ready Instances | Pre-warmed instances | Always-ready instances (per-function group) | Map current pre-warm config |
| Deployment Package | Runs from package | Deployment via managed blob storage | New storage requirement |
| Max Instance Count | Configurable | Configurable burst limit | Set appropriate burst limit |
| Durable Functions | ✅ Supported | ✅ Supported (verify extension version) | Test task hub migration |
| Python Version | Verify current | 3.8–3.11 (check latest) | Confirm version compatibility |
| HTTP Trigger Concurrency | Host-level | Per-instance concurrency | Tune concurrency settings |
| Timer/Queue Triggers | Supported | Supported | Verify trigger-specific limits |
| Instance Memory | Fixed by SKU | 2048 MB or 4096 MB (selectable) | Choose appropriate memory size |

---

## Phase 1: Complete Security Audit (Current Function App)

> **Objective**: Document every security-relevant setting on the existing EP1 function app to ensure parity (or improvement) on the new Flex Consumption function app.

Complete each section of the [Security Audit Checklist](#1-security-audit-checklist) above. Use the [CLI commands in Section 6](#6-recommended-cli-commands-for-audit) to extract data.

**Step 1.1 — Identity & Auth Audit**
1. Open the function app in Azure Portal → **Identity** blade
2. Record system-assigned identity status (Enabled/Disabled) and Object ID
3. Record any user-assigned identities (name, client ID, object ID)
4. Navigate to **Access control (IAM)** → **Role assignments** → export the list
5. Navigate to **Authentication** blade → document provider configuration
6. Navigate to **App keys** → document key names and rotation policy

**Step 1.2 — Secrets & Configuration Audit**
1. Navigate to **Configuration** → **Application settings**
2. Export all setting **names** (do NOT export values to any shared document)
3. Identify which settings use `@Microsoft.KeyVault(...)` references
4. Flag any settings that appear to contain plain-text secrets (connection strings, passwords, keys)
5. Cross-reference the managed identity against Key Vault access policies

**Step 1.3 — TLS & Transport Audit**
1. Navigate to **Configuration** → **General settings**
2. Record: Minimum TLS Version, HTTPS Only, Client Certificate Mode, FTP State, Remote Debugging
3. Navigate to **Custom domains** → document any custom domains and SSL bindings

**Step 1.4 — Network Security Audit**
1. Navigate to **Networking** blade
2. Document: Public network access, Access restrictions (main site + SCM), Private endpoints, VNet integration
3. Navigate to the VNet resource → **Subnets** → record NSG and Route Table for each relevant subnet
4. Navigate to the Route Table → document all UDR rules
5. Check if a NAT gateway is associated with the integration subnet
6. Document DNS settings on the VNet (custom DNS servers vs Azure default)

**Step 1.5 — Storage Security Audit**
1. Identify all storage accounts from app settings (look for `AzureWebJobsStorage`, `DurableTask:StorageProvider:ConnectionName`, etc.)
2. For each storage account:
   - Navigate to **Networking** → document firewall rules (VNet rules, IP rules, exceptions)
   - Navigate to **Private endpoint connections** → list all PEs
   - Navigate to **Access keys** → note if keys are the auth method (vs identity-based)
   - Navigate to **Encryption** → document key management (Microsoft vs customer-managed)

**Step 1.6 — Monitoring & Compliance Audit**
1. Navigate to **Diagnostic settings** → document where logs are sent
2. Navigate to **Application Insights** → record connection string and sampling config
3. Check **Microsoft Defender for Cloud** → note any recommendations for this resource
4. Check **Azure Policy** → find any non-compliant policies on this resource

**Step 1.7 — Application Security Audit**
1. Navigate to **API** → **CORS** → document allowed origins
2. Confirm remote debugging is disabled
3. Confirm FTP is disabled

---

## Phase 2: Pre-Migration Preparation

### Step 2.1 — Flex Consumption Feasibility Check

1. Confirm Flex Consumption is **GA in your target region** — check [Azure region availability](https://learn.microsoft.com/en-us/azure/azure-functions/flex-consumption-plan#region-support)
2. Validate all triggers and bindings used are supported on Flex Consumption
3. Validate private endpoint support (inbound) is available in your region
4. Confirm Python runtime version compatibility

---

### Step 2.2 — Create New Subnet for Flex Consumption (Azure Portal)

> [!WARNING]
> Flex Consumption uses subnet delegation `Microsoft.App/environments`, which is **different** from EP1's `Microsoft.Web/serverFarms`. You **cannot reuse the same subnet**. You must create a new subnet in the same VNet.

> [!IMPORTANT]
> **What you CAN reuse** (same VNet):
> - ✅ Existing VNet — the new subnet is added within it
> - ✅ Existing Private Endpoints for downstream services (Storage, Key Vault, SQL, etc.) — they resolve via the same Private DNS Zones linked to the VNet
> - ✅ Existing User Defined Routes (UDRs) — associate the same Route Table to the new subnet
> - ✅ Existing NAT Gateway — linked via the Route Table / subnet association
> - ✅ Existing NSGs — associate the same or a new NSG to the new subnet
>
> The **only new resources** needed are:
> - The subnet itself (with `Microsoft.App/environments` delegation)
> - A new Private Endpoint for the new function app's inbound access

#### Portal Steps — Create a New Subnet

1. Navigate to the **Azure Portal** → search for your **Virtual Network** resource
2. In the left menu, click **Subnets**
3. Click **+ Subnet** at the top

4. Fill in the subnet details:

   | Field | Value | Notes |
   |---|---|---|
   | **Name** | e.g. `snet-funcapp-flex` | Use your naming convention |
   | **Subnet address range** | Choose an available `/24` CIDR block | Minimum `/24` for Flex Consumption. Example: if your VNet is `10.0.0.0/16` and `10.0.1.0/24` is free, use that |
   | **NAT Gateway** | Select existing NAT gateway if used | Only if your outbound traffic requires NAT |
   | **Network Security Group** | Select existing NSG or create new | Recommended: apply the same or a tailored NSG |
   | **Route Table** | Select the **existing Route Table** used by the EP1 subnet | This ensures the same UDRs (including NAT gateway routing) apply |
   | **Subnet Delegation** | Select `Microsoft.App/environments` | ⚠️ Critical — this is the required delegation for Flex Consumption |
   | **Service Endpoints** | Add any service endpoints that match the EP1 subnet | Only if you use service endpoints (not needed if using private endpoints exclusively) |

5. Click **Save**

6. **Verify the subnet**:
   - Click on the newly created subnet name
   - Confirm delegation shows `Microsoft.App/environments`
   - Confirm the Route Table is correctly associated
   - Confirm the NSG is correctly associated

#### Post-Creation Verification

- [ ] Subnet created with correct CIDR range (minimum `/24`)
- [ ] Delegation set to `Microsoft.App/environments`
- [ ] Route Table associated (same as EP1 subnet's route table)
- [ ] NSG associated and rules reviewed
- [ ] NAT gateway associated (if applicable)
- [ ] No CIDR overlap with existing subnets
- [ ] Sufficient IP addresses for expected instance count (each Flex Consumption instance uses one IP)

> [!TIP]
> To calculate required IPs: maximum burst instance count + a few spare. A `/24` provides 251 usable IPs, which is typically sufficient.

---

### Step 2.3 — Storage Design

1. Decide: reuse existing storage accounts or create new ones
2. For Durable Functions: plan a **new task hub name** to avoid conflicts during parallel run
3. Verify storage private endpoints are accessible from the new subnet (same VNet = same DNS resolution)
4. If storage accounts use VNet firewall rules, **add the new subnet** to the allowed list:
   - Navigate to each storage account → **Networking** → **Firewalls and virtual networks**
   - Under **Virtual networks**, click **+ Add existing virtual network**
   - Select the new Flex Consumption subnet
   - Click **Save**

### Step 2.4 — Identity Design

1. Decide: carry over existing **user-assigned managed identity** or create a new one
   - If reusing: no downstream RBAC changes needed
   - If creating new: pre-configure RBAC on all downstream resources (Key Vault, Storage, Service Bus, SQL, etc.)
2. Update Key Vault access policies for the new identity (if creating new)
3. Document the identity decision for the implementing engineer

### Step 2.5 — DNS Planning

1. New function app will need a **new Private Endpoint** for inbound access
2. This will create a new A record in the existing `privatelink.azurewebsites.net` Private DNS Zone
3. Verify the Private DNS Zone is already linked to the VNet (it should be if the existing function app uses it)
4. If using custom DNS servers, ensure conditional forwarders are in place for `privatelink.azurewebsites.net`

---

## Phase 3: Provision New Function App

### Step 3.1 — Create Flex Consumption Function App

> Recommended: use IaC (Bicep/Terraform/ARM) for auditability and repeatability.

Configure the following:

| Setting | Value | Notes |
|---|---|---|
| Plan type | Flex Consumption | — |
| Region | Same as current function app | Must be a region where Flex Consumption is GA |
| Runtime stack | Python | — |
| Python version | Match current or upgrade | Verify compatibility |
| Instance memory | 2048 MB or 4096 MB | Choose based on workload |
| Always-ready instances | Map from EP1 pre-warmed count | Per function group |
| Max instance count | Set appropriate burst limit | Based on load testing |

### Step 3.2 — Configure Networking

Perform the following in order:

1. **VNet Integration**
   - Navigate to the new function app → **Networking** → **VNet integration**
   - Select the **new subnet** (`snet-funcapp-flex`) created in Step 2.2
   - Enable **Route All** if required (to route all outbound traffic through VNet)

2. **Private Endpoint (Inbound)**
   - Navigate to the new function app → **Networking** → **Private endpoint connections**
   - Click **+ Add**
   - Select the appropriate subnet for the PE (can be the existing PE subnet used by other services)
   - Set the private DNS zone to `privatelink.azurewebsites.net`
   - The A record will be auto-registered if DNS zone integration is enabled

3. **Disable Public Access**
   - Navigate to **Networking** → **Public network access** → set to **Disabled**

4. **Access Restrictions**
   - Mirror the access restriction rules from the EP1 function app
   - Apply to both main site and SCM site

### Step 3.3 — Configure Security Settings

Navigate to the new function app → **Configuration** → **General settings**:

- [ ] Set **Minimum TLS Version** to `1.2`
- [ ] Set **HTTPS Only** to `On`
- [ ] Set **FTP State** to `Disabled`
- [ ] Set **Remote Debugging** to `Off`
- [ ] Set **Client Certificate Mode** to match EP1 configuration

Navigate to **API** → **CORS**:
- [ ] Set allowed origins to match EP1 (or tighten)
- [ ] Do NOT use wildcard `*` in production

### Step 3.4 — Configure Identity & Secrets

1. **Managed Identity**
   - Navigate to **Identity** blade
   - Enable system-assigned identity (if used) or attach the user-assigned identity
   - Record the Object ID

2. **RBAC**
   - For the new identity, assign the same roles on downstream resources:
     - Key Vault (Key Vault Secrets User, Key Vault Crypto User, etc.)
     - Storage accounts (Storage Blob Data Contributor, etc.)
     - Service Bus, SQL, Cosmos, etc.

3. **App Settings**
   - Migrate all app settings from EP1 to the new function app
   - Ensure all Key Vault references point to the correct Key Vault and use the new managed identity
   - Validate each Key Vault reference resolves (green checkmark in the portal)

4. **Identity-based Storage Connection** (if applicable)
   - Configure `AzureWebJobsStorage` to use managed identity instead of connection string

### Step 3.5 — Configure Monitoring

1. **Application Insights**
   - Navigate to **Application Insights** blade
   - Connect to the existing Application Insights instance (or create new)
   - Set `APPLICATIONINSIGHTS_CONNECTION_STRING` in app settings

2. **Diagnostic Settings**
   - Navigate to **Diagnostic settings** → **+ Add diagnostic setting**
   - Select: `FunctionAppLogs`, `AllMetrics`
   - Send to: Log Analytics workspace (same as EP1)

3. **Alerts**
   - Recreate any alert rules from EP1 for the new function app

### Step 3.6 — Configure Storage

1. Set connection strings / app settings for all storage accounts used
2. For Durable Functions:
   - Set a **new task hub name** (e.g. `flex-taskhub`) to avoid conflicts with EP1
   - Point to the correct storage account
3. Verify storage connectivity:
   - Test from the function app's Kudu console (Advanced Tools) — attempt to reach storage via private endpoint
   - If storage uses VNet firewall rules, confirm the new subnet was added (Step 2.3)

---

## Phase 4: Deploy & Validate

### Step 4.1 — Deploy Application Code

1. Deploy Python code via CI/CD pipeline or `az functionapp deploy`
2. If using ZIP deploy: `az functionapp deployment source config-zip -g <rg> -n <app> --src <package.zip>`
3. Verify deployment package is correctly stored in managed blob
4. Check the function app → **Functions** blade → confirm all functions are listed and enabled

### Step 4.2 — Functional Testing

- [ ] Test each HTTP trigger function via private endpoint
- [ ] Test Timer trigger functions — verify they fire on schedule
- [ ] Test Queue/Service Bus trigger functions — send test messages
- [ ] Test Durable Function orchestrations — start an orchestration, verify completion
- [ ] Test cold-start performance and always-ready instance behavior
- [ ] Compare function execution logs between EP1 and Flex Consumption

### Step 4.3 — Security Validation

- [ ] **Public access blocked** — attempt to call the function app from outside the VNet → should fail
- [ ] **Private endpoint works** — call the function from within the VNet → should succeed
- [ ] **Outbound via VNet** — from the function, make an outbound HTTP call and verify the source IP matches the NAT gateway public IP
- [ ] **Storage access** — verify the function can read/write to storage accounts via private endpoints
- [ ] **Key Vault access** — verify Key Vault references resolve (green checkmark in Configuration blade)
- [ ] **Managed identity** — verify the function can authenticate to all downstream services
- [ ] **No unexpected egress** — review outbound connections in Application Insights or Network Watcher flow logs

### Step 4.4 — Performance Baseline

- [ ] Measure cold-start times (Flex Consumption vs EP1)
- [ ] Measure throughput under load for key functions
- [ ] Validate concurrency settings — adjust per-instance concurrency if needed
- [ ] Monitor memory usage relative to instance memory size selection

---

## Phase 5: Cutover & Decommission

### Step 5.1 — Traffic Cutover

1. Update DNS / private endpoint references for all consumers of the function app
2. If using API Management or Front Door, update the backend pool to point to the new function app's PE
3. Monitor error rates and latency closely during cutover

### Step 5.2 — Parallel Run Period

1. Keep EP1 function app running with **triggers disabled** for rollback capability
2. Monitor the new Flex Consumption function app for **1–2 weeks**
3. Compare Application Insights telemetry: error rates, latency, throughput

### Step 5.3 — Decommission Old Function App

Once confident the migration is stable:

1. **Disable** the EP1 function app (do not delete immediately)
2. Wait an additional buffer period (e.g. 1 week)
3. **Delete** the EP1 function app
4. **Clean up** old resources:
   - [ ] Delete old private endpoint for the EP1 function app
   - [ ] Remove the old DNS A record from `privatelink.azurewebsites.net`
   - [ ] Remove subnet delegation from the old subnet (or delete the subnet if no longer needed)
   - [ ] Remove old RBAC assignments (if using a new identity)
   - [ ] Remove old Key Vault access policies (if applicable)
5. **Update documentation** and runbooks to reflect the new architecture

---

## 6. Recommended CLI Commands for Audit

> [!TIP]
> Use these Azure CLI commands to extract audit data from the existing function app. Replace `<rg>`, `<app>`, `<sub>`, and `<storage>` with your values.

```bash
# ── Identity ──
az functionapp identity show -g <rg> -n <app>
az role assignment list --scope /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Web/sites/<app>

# ── Networking ──
az functionapp show -g <rg> -n <app> --query "publicNetworkAccess"
az webapp config access-restriction show -g <rg> -n <app>
az network private-endpoint list -g <rg> --query "[?contains(privateLinkServiceConnections[0].privateLinkServiceId,'<app>')]"
az functionapp vnet-integration list -g <rg> -n <app>

# ── TLS & Security ──
az functionapp config show -g <rg> -n <app> \
  --query "{minTlsVersion:minTlsVersion, httpsOnly:httpsOnly, ftpsState:ftpsState, http20Enabled:http20Enabled}"
az functionapp show -g <rg> -n <app> --query "clientCertEnabled"

# ── App Settings (names only, for security) ──
az functionapp config appsettings list -g <rg> -n <app> --query "[].name"

# ── CORS ──
az functionapp cors show -g <rg> -n <app>

# ── Diagnostic Settings ──
az monitor diagnostic-settings list \
  --resource /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Web/sites/<app>

# ── Storage Account Firewall ──
az storage account show -n <storage> --query "networkRuleSet"

# ── NSG Rules on Subnet ──
az network nsg rule list --nsg-name <nsg-name> -g <rg> -o table

# ── Route Table ──
az network route-table route list --route-table-name <rt-name> -g <rg> -o table

# ── Private DNS Zone Records ──
az network private-dns record-set a list -g <rg> -z privatelink.azurewebsites.net -o table

# ── Private DNS Zone VNet Links ──
az network private-dns link vnet list -g <rg> -z privatelink.azurewebsites.net -o table
```

---

## 7. Resource Reuse Reference

The following table summarises what can be reused when creating a new subnet for Flex Consumption:

| Resource | Reuse? | Action Needed |
|---|---|---|
| **VNet** | ✅ Same VNet | Add new `/24` subnet with `Microsoft.App/environments` delegation |
| **UDR / Route Table** | ✅ Same Route Table | Associate it to the new subnet during subnet creation |
| **NAT Gateway** | ✅ Same NAT Gateway | Already linked via Route Table / subnet association |
| **NSG** | ✅ Same or new NSG | Associate to the new subnet; review rules for Flex Consumption requirements |
| **PEs to downstream services** | ✅ No change | DNS resolves via same Private DNS Zones linked to VNet |
| **PE for function app (inbound)** | ⚠️ New PE needed | Create a new PE for the new Flex Consumption function app |
| **Private DNS Zones** | ✅ Same zones | New A record auto-registered for the new PE |
| **Key Vault** | ✅ Same Key Vault | Add RBAC/access policy for new managed identity (if creating new) |
| **Storage Accounts** | ✅ Same accounts | Add new subnet to storage firewall rules (if using VNet rules) |
| **Application Insights** | ✅ Same instance | Point new function app to same connection string |
| **Log Analytics Workspace** | ✅ Same workspace | Configure diagnostic settings on new function app |

---

## Open Questions (Resolve Before Implementation)

> [!IMPORTANT]
> The implementing engineer should resolve these questions before starting Phase 2:
>
> 1. **Region** — Is Flex Consumption GA in your target Azure region?
> 2. **Private Endpoints for Inbound** — Is private endpoint support confirmed for Flex Consumption in your region?
> 3. **Durable Functions** — Are you using Durable Functions? Do you need data continuity from the old task hub, or can you start fresh?
> 4. **IaC** — Are you using ARM/Bicep/Terraform? If so, prepare templates for the new resources.
> 5. **CI/CD Pipeline** — What is the current deployment method? Update it for Flex Consumption's deployment model.
> 6. **Downstream Dependencies** — Do any downstream services have firewall rules referencing the current function app's subnet? If so, add the new subnet to their allow lists.
> 7. **Managed Identity** — Reuse existing user-assigned identity or create new? If creating new, plan RBAC assignments across all downstream resources.
