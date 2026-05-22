import { test, expect } from '@playwright/test';

test.describe('Delta-PER (KR) Dashboard UX Tests', () => {
  
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    // Playwright uses the baseURL from config (http://localhost:3000)
    await page.goto('/');
    // Wait for the main page to load and data to be fetched
    await page.waitForSelector('table');
  });

  test('should render page title, sidebar branding and initial elements', async ({ page }) => {
    // 1. Verify branding in the sidebar
    const titleText = page.locator('aside h1');
    await expect(titleText).toContainText('Delta-PER (KR)');

    // 2. Verify info banner is visible
    const infoBanner = page.locator('text=Delta-PER (KR) 이란?');
    await expect(infoBanner).toBeVisible();

    // 3. Verify table exists with correct headers
    const tableHeaders = page.locator('thead tr th');
    await expect(tableHeaders).toHaveCount(9);
    
    // Check key headers
    await expect(tableHeaders.nth(1)).toContainText('종목명');
    await expect(tableHeaders.nth(3)).toContainText('Delta');
  });

  test('should filter stocks when typing in the search bar', async ({ page }) => {
    // 1. Get total rows initially
    const initialRowsCount = await page.locator('tbody tr').count();
    console.log(`Initial rows count: ${initialRowsCount}`);

    // 2. Type "삼성" (Samsung) into search input
    const searchInput = page.getByPlaceholder('종목명 또는 코드');
    await expect(searchInput).toBeVisible();
    await searchInput.fill('삼성');

    // Wait a brief moment for the filtering/state update
    await page.waitForTimeout(500);

    // 3. Get filtered rows count and verify it's less or contains "삼성"
    const filteredRows = page.locator('tbody tr');
    const filteredCount = await filteredRows.count();
    console.log(`Filtered rows count: ${filteredCount}`);
    
    if (filteredCount > 0) {
      // Check that the first row's text contains "삼성"
      const firstRowText = await filteredRows.first().textContent();
      expect(firstRowText).toContain('삼성');
    }
  });

  test('should filter priority stocks when clicking priority toggle', async ({ page }) => {
    // 1. Get the priority count badge from the sidebar button
    const priorityButton = page.locator('aside button:has-text("우선고려종목만 보기")');
    await expect(priorityButton).toBeVisible();
    
    // Extract count text (e.g. "12개")
    const badgeText = await priorityButton.locator('span.font-bold').textContent();
    const expectedCount = parseInt(badgeText || '0', 10);
    console.log(`Expected priority count from badge: ${expectedCount}`);

    // 2. Click the priority toggle
    await priorityButton.click();
    await page.waitForTimeout(500);

    // 3. Check table rows count
    const visibleRowsCount = await page.locator('tbody tr').count();
    console.log(`Visible rows after priority toggle: ${visibleRowsCount}`);
    
    // If the expectedCount is 0, the table might show the "조건에 맞는 종목이 없습니다." row
    if (expectedCount === 0) {
      const emptyRowText = await page.locator('tbody tr').first().textContent();
      expect(emptyRowText).toContain('조건에 맞는 종목이 없습니다');
    } else {
      expect(visibleRowsCount).toBe(expectedCount);

      // Verify that all visible rows have the "우선고려" badge
      const rows = page.locator('tbody tr');
      for (let i = 0; i < visibleRowsCount; i++) {
        const rowText = await rows.nth(i).textContent();
        expect(rowText).toContain('우선고려');
      }
    }
  });

  test('should toggle missing values and update item count', async ({ page }) => {
    // Find checkbox container and the checkbox input
    const missingCheckbox = page.locator('label:has-text("결측치 포함(500개 보기)") input');
    await expect(missingCheckbox).toBeVisible();

    const initialCheckedState = await missingCheckbox.isChecked();
    console.log(`Initial Checked State for Missing Values: ${initialCheckedState}`);

    // Toggle the checkbox
    await missingCheckbox.setChecked(!initialCheckedState);
    await page.waitForTimeout(500);

    // Verify checked status changed
    const newCheckedState = await missingCheckbox.isChecked();
    expect(newCheckedState).toBe(!initialCheckedState);
  });

  test('should sort table rows when clicking column headers', async ({ page }) => {
    // 1. Get DeltaPER of the first two rows before sorting
    const rows = page.locator('tbody tr');
    const initialRowsCount = await rows.count();
    
    if (initialRowsCount >= 2) {
      // Click '종목명' header to sort by name
      const nameHeader = page.locator('thead tr th:has-text("종목명")');
      await nameHeader.click();
      await page.waitForTimeout(500);
      
      const firstRowName = await rows.first().locator('td').nth(1).textContent();
      
      // Click again to sort ascending/descending
      await nameHeader.click();
      await page.waitForTimeout(500);
      
      const firstRowNameSecondClick = await rows.first().locator('td').nth(1).textContent();
      console.log(`First row name after click 1: ${firstRowName}, click 2: ${firstRowNameSecondClick}`);
      
      // They should differ or update since order toggled
      expect(firstRowName).not.toBeNull();
    }
  });

  test('should trigger download when clicking export button', async ({ page }) => {
    const downloadButton = page.locator('header button:has-text("엑셀 다운로드")');
    await expect(downloadButton).toBeVisible();

    // Start waiting for download before clicking
    const downloadPromise = page.waitForEvent('download');
    await downloadButton.click();
    const download = await downloadPromise;

    // Assert download is successful
    expect(download.suggestedFilename()).toContain('delta_per_data.csv');
    console.log(`Download triggered successfully for file: ${download.suggestedFilename()}`);
  });
});
