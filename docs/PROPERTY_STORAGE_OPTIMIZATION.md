# Property Collection Storage Optimization Analysis

## Summary

After analyzing the Property model structure and usage patterns, here are opportunities to reduce property JSON size dramatically:

## 🔍 Duplicate/Redundant Fields

### 1. **Address Fields** - Potential Savings: ~200-500 bytes per property

**Duplicate fields:**
- `address.formatted_address` - Full formatted address (e.g., "123 Main St, City, State 12345")
- `address.full_street_line` - Similar to formatted_address
- `address.street` + `address.city` + `address.state` + `address.zip_code` - Component fields

**Recommendation:**
- Keep only `address.formatted_address` (most commonly used)
- Derive `full_street_line` if needed (it's usually same or similar)
- Keep component fields (`street`, `city`, `state`, `zip_code`) for filtering/searching
- **Savings**: ~100-200 bytes per property (if we remove `full_street_line`)

### 2. **URL/ID Fields** - Potential Savings: ~200-400 bytes per property

**Duplicate fields:**
- `property_url` - URL to property listing
- `permalink` - Usually same as property_url or very similar
- `listing_id` - Listing identifier
- `mls_id` - MLS identifier (often same as listing_id)

**Recommendation:**
- Keep `mls_id` (primary identifier, used for searches)
- Keep `property_url` (most useful for linking)
- Remove `permalink` (duplicate of property_url) - **Savings: ~50-150 bytes**
- Remove `listing_id` if it's same as `mls_id` - **Savings: ~50-100 bytes**

### 3. **Description Fields** - Potential Savings: Variable

**Possible duplicates:**
- `description.text` - Full property description (can be VERY large - 5-50KB)
- `description.full_description` - Usually same as `text`
- `description.summary` - Usually a subset of `text`

**Recommendation:**
- Keep only `description.text` (most commonly used)
- Remove `description.full_description` if same as `text` - **Savings: ~5-50KB per property**
- Keep `description.summary` only if it's actually a summary (smaller)

## 📦 Large Fields (High Storage Impact)

### 1. **alt_photos** - Potential Savings: 1-10KB per property

**Issue:**
- List of photo URLs (could be 20-50 photos)
- Each URL is ~100-200 bytes
- Total: ~2-10KB per property

**Recommendation:**
- **Option A**: Store only `primary_photo`, fetch `alt_photos` on-demand from source
- **Option B**: Limit to first 10 photos (most important)
- **Savings**: ~1-9KB per property

### 2. **description.text** - Potential Savings: 2-40KB per property

**Issue:**
- Property descriptions can be VERY long (2-50KB)
- Full descriptions are needed for keyword scanning
- But may not need full text stored

**Recommendation:**
- **Option A**: Store first 500 characters + full text hash (fetch from source if needed)
- **Option B**: Keep full text but compress it (gzip - ~70% reduction)
- **Option C**: Move full descriptions to separate collection, store summary in main
- **Savings**: ~2-40KB per property (with truncation/compression)

### 3. **monthly_fees**, **one_time_fees**, **tax_history**, **nearby_schools**

**Issue:**
- Arrays of dictionaries with detailed information
- Each can be 500 bytes - 2KB per property
- Rarely used for motivated seller scoring

**Recommendation:**
- **If not used in enrichment/scoring**: Remove entirely - **Savings: ~2-8KB per property**
- **If needed occasionally**: Move to separate collection, store only IDs/references in main
- **Savings**: ~2-8KB per property

## 🗑️ Rarely Used Fields

### 1. **Contact Information Details** - Potential Savings: ~200-500 bytes per property

**Fields:**
- `agent.agent_mls_set` - Rarely used
- `agent.agent_nrds_id` - Rarely used  
- `office.office_mls_set` - Rarely used
- `builder` - Entire section rarely populated (< 5% of properties)

**Recommendation:**
- Remove `agent.agent_mls_set` and `agent.agent_nrds_id` if not used - **Savings: ~50-100 bytes**
- Remove `office.office_mls_set` if not used - **Savings: ~50 bytes**
- Remove `builder` section if < 5% populated - **Savings: ~100-300 bytes**

### 2. **Metadata Fields**

**Fields:**
- `is_comp` - Default False, rarely True
- `source` - Always "homeharvest"

**Recommendation:**
- Remove `is_comp` if rarely True - **Savings: ~10 bytes** (but may be needed)
- Remove `source` if always same - **Savings: ~20 bytes**

## 💾 Storage Optimization Strategies

### Strategy 1: Remove Duplicate/Unused Fields (Low Risk)

**Immediate Actions:**
1. Remove `permalink` (duplicate of `property_url`)
2. Remove `listing_id` if same as `mls_id`
3. Remove `address.full_street_line` (duplicate of `formatted_address`)
4. Remove `description.full_description` if same as `description.text`
5. Remove `agent.agent_mls_set`, `agent.agent_nrds_id`, `office.office_mls_set`
6. Remove `builder` if < 5% populated
7. Remove `source` if always "homeharvest"

**Estimated Savings**: ~500-1,500 bytes per property = **~0.5-1.5KB per property**

For 1,000 properties: **~500KB - 1.5MB**

### Strategy 2: Optimize Large Fields (Medium Risk)

**Actions:**
1. **alt_photos**: Limit to 10 photos or fetch on-demand
   - Savings: ~1-9KB per property
2. **description.text**: Truncate to 500 chars or compress
   - Savings: ~2-40KB per property  
3. **Remove unused arrays**: monthly_fees, one_time_fees, tax_history, nearby_schools if not used
   - Savings: ~2-8KB per property

**Estimated Savings**: ~5-57KB per property = **~5-57KB per property**

For 1,000 properties: **~5-57MB** ⚠️ **Significant savings!**

### Strategy 3: Move Rarely Used Data to Separate Collection (High Impact)

**Actions:**
1. Create `property_details` collection for:
   - Full description text
   - All alt_photos
   - monthly_fees, one_time_fees, tax_history, nearby_schools
2. Store only references/IDs in main properties collection
3. Fetch details on-demand when needed

**Estimated Savings**: ~10-60KB per property

For 1,000 properties: **~10-60MB** 🚀 **Massive savings!**

## 📊 Estimated Total Savings

| Strategy | Savings per Property | Total Savings (1000 props) |
|----------|---------------------|---------------------------|
| **Remove duplicates** | 0.5-1.5 KB | 0.5-1.5 MB |
| **Optimize large fields** | 5-57 KB | 5-57 MB |
| **Move to separate collection** | 10-60 KB | 10-60 MB |
| **Total Potential** | **15-118 KB** | **15-118 MB** |

**Current property size**: ~120KB
**Optimized size**: ~5-105KB
**Reduction**: **~12-96%** 🎯

## ⚠️ Implementation Considerations

### Low Risk Changes (Safe to Remove):
- ✅ `permalink` (duplicate)
- ✅ `listing_id` (if same as mls_id)
- ✅ `address.full_street_line` (duplicate)
- ✅ `agent.agent_mls_set`, `agent.agent_nrds_id`
- ✅ `office.office_mls_set`
- ✅ `source` (if always same)

### Medium Risk (Need Verification):
- ⚠️ `description.full_description` (verify if different from `text`)
- ⚠️ `builder` (verify usage rate)
- ⚠️ `monthly_fees`, `one_time_fees`, `tax_history`, `nearby_schools` (verify usage)

### High Risk (Requires Code Changes):
- 🔴 Truncate `description.text` (may break keyword scanning)
- 🔴 Limit `alt_photos` (may break UI that expects all photos)
- 🔴 Move to separate collection (requires API changes)

## 🚀 Recommended Implementation Plan

### Phase 1: Quick Wins (Low Risk)
1. Remove duplicate fields (`permalink`, `listing_id`, `full_street_line`, `full_description`)
2. Remove rarely used contact fields
3. **Estimated time**: 1-2 hours
4. **Estimated savings**: ~0.5-1.5MB

### Phase 2: Optimize Large Fields (Medium Risk)
1. Limit `alt_photos` to 10 photos
2. Remove unused arrays if confirmed not needed
3. **Estimated time**: 2-4 hours
4. **Estimated savings**: ~5-57MB

### Phase 3: Move to Separate Collection (High Impact)
1. Create `property_details` collection
2. Move large/rarely-used fields
3. Update APIs to fetch on-demand
4. **Estimated time**: 4-8 hours
5. **Estimated savings**: ~10-60MB

## 📝 Next Steps

1. **Run analysis script** to verify actual field usage:
   ```bash
   python scripts/analyze_property_storage.py --sample 100
   ```

2. **Verify field usage** in backend/frontend:
   - Check if `permalink`, `listing_id`, `full_street_line` are used
   - Check if `monthly_fees`, `tax_history`, etc. are accessed
   - Check if `alt_photos` beyond first 10 are displayed

3. **Start with Phase 1** (low risk, immediate savings)

4. **Monitor storage** after each phase

5. **Consider Phase 3** if storage is still an issue

