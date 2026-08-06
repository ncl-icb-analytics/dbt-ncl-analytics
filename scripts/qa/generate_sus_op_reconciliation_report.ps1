param(
    [Parameter(Mandatory)]
    [string]$FiguresPath,

    [Parameter(Mandatory)]
    [string[]]$Recipients,

    [Parameter(Mandatory)]
    [string]$Sender,

    [string]$OutputDirectory = (Join-Path $PSScriptRoot '..\..\reports\sus_op_reconciliation')
)

$ErrorActionPreference = 'Stop'
$outputPath = [System.IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null

$xlsxPath = Join-Path $outputPath 'SUS_OP_ERNI_vs_Snowflake_Reconciliation.xlsx'
$emlPath = Join-Path $outputPath 'SUS_OP_Reconciliation_Review.eml'

$resolvedFiguresPath = (Resolve-Path -LiteralPath $FiguresPath).Path
$inputRows = @(Import-Csv -LiteralPath $resolvedFiguresPath)
if (-not $inputRows) { throw "Figures CSV contains no data: $resolvedFiguresPath" }

$requiredColumns = @('FinancialYear', 'Provider', 'ERNI', 'Snowflake')
$actualColumns = @($inputRows[0].PSObject.Properties.Name)
$missingColumns = @($requiredColumns | Where-Object { $_ -notin $actualColumns })
if ($missingColumns) {
    throw "Figures CSV is missing required columns: $($missingColumns -join ', ')"
}

$detailRows = foreach ($row in $inputRows) {
    $erni = [long]$row.ERNI
    $snowflake = [long]$row.Snowflake
    [pscustomobject]@{
        FinancialYear = $row.FinancialYear.Trim()
        Provider = $row.Provider.Trim()
        ERNI = $erni
        Snowflake = $snowflake
        Difference = $snowflake - $erni
        DifferencePct = if ($erni) { ($snowflake - $erni) / $erni } else { $null }
    }
}

$yearKeys = @($detailRows.FinancialYear | Sort-Object -Unique)
$yearLabels = @{}
foreach ($year in $yearKeys) {
    if ($year -notmatch '^\d{4}$') { throw "FinancialYear must use YYZZ format: $year" }
    $yearLabels[$year] = "20$($year.Substring(0, 2))/$($year.Substring(2, 2))"
}

$yearSummary = foreach ($year in $yearKeys) {
    $rows = $detailRows | Where-Object FinancialYear -eq $year
    $erni = ($rows | Measure-Object ERNI -Sum).Sum
    $snowflake = ($rows | Measure-Object Snowflake -Sum).Sum
    [pscustomobject]@{
        FinancialYear = $yearLabels[$year]
        ERNI = $erni
        Snowflake = $snowflake
        Difference = $snowflake - $erni
        DifferencePct = if ($erni) { ($snowflake - $erni) / $erni } else { $null }
    }
}

$providerDisplay = @($detailRows.Provider | Sort-Object -Unique) -join ', '
$findings = @(foreach ($item in $yearSummary) {
    if ($null -eq $item.DifferencePct) {
        "$($item.FinancialYear): Investigate; ERNI has zero rows."
    } else {
        $absPct = [math]::Abs($item.DifferencePct)
        $status = if ($absPct -le 0.005) { 'Close match' } elseif ($absPct -le 0.02) { 'Review' } else { 'Investigate' }
        "$($item.FinancialYear): $status; difference $($item.Difference.ToString('N0')) ($($item.DifferencePct.ToString('P2')))."
    }
})
$reviewYears = @($yearSummary | Where-Object { $null -eq $_.DifferencePct -or [math]::Abs($_.DifferencePct) -gt 0.005 } | ForEach-Object FinancialYear)
$steps = @(
    'Confirm that ERNI and Snowflake have equivalent coverage for every comparison period.',
    $(if ($reviewYears) { "Investigate flagged years by financial month: $($reviewYears -join ', ')." } else { 'No year-level differences exceed the close-match threshold.' }),
    'Compare total rows with distinct encounter and attendance identifiers.',
    'Use a close-matching month as a control for record-level reconciliation.'
)

$sqlErni = @'
SELECT
    zFinancialYear AS financial_year,
    LEFT(LTRIM(RTRIM(OrganisationCodeCodeofProvider)), 3) AS provider,
    COUNT_BIG(*) AS erni_rows
FROM [dmic_sus_pbrmart].[dbo].[consolidated_ns_op]
WHERE LEFT(LTRIM(RTRIM(OrganisationCodeCodeofProvider)), 3)
      IN ('RYJ', 'RQM', 'RAS', 'R1K')
GROUP BY
    zFinancialYear,
    LEFT(LTRIM(RTRIM(OrganisationCodeCodeofProvider)), 3)
ORDER BY financial_year, provider;
'@

$sqlSnowflake = @'
SELECT
    zfinancialyear AS financial_year,
    LEFT(TRIM(organisation_code_code_of_provider), 3) AS provider,
    COUNT(*) AS snowflake_rows
FROM DEV__REPORTING.COMMISSIONING_REPORTING.FCT_SUS_OP_CONSOLIDATED
WHERE LEFT(TRIM(organisation_code_code_of_provider), 3)
      IN ('RYJ', 'RQM', 'RAS', 'R1K')
GROUP BY 1, 2
ORDER BY 1, 2;
'@

$excel = $null
try {
    $excel = New-Object -ComObject Excel.Application
    $excel.Visible = $false
    $excel.DisplayAlerts = $false
    $workbook = $excel.Workbooks.Add()

    while ($workbook.Worksheets.Count -lt 4) { $null = $workbook.Worksheets.Add() }
    while ($workbook.Worksheets.Count -gt 4) { $workbook.Worksheets.Item($workbook.Worksheets.Count).Delete() }

    $summarySheet = $workbook.Worksheets.Item(1)
    $summarySheet.Name = 'Executive Summary'
    $yearSheet = $workbook.Worksheets.Item(2)
    $yearSheet.Name = 'Year Summary'
    $detailSheet = $workbook.Worksheets.Item(3)
    $detailSheet.Name = 'Provider Detail'
    $sqlSheet = $workbook.Worksheets.Item(4)
    $sqlSheet.Name = 'SQL Used'

    $navy = 0x6B351F
    $blue = 0xD9A23B
    $lightRed = 0xCCCCFF
    $lightAmber = 0xCCF2FF
    $lightGreen = 0xE2F0D9
    $white = 0xFFFFFF

    $summarySheet.Range('A1:F1').Merge()
    $summarySheet.Range('A1').Value2 = 'SUS Outpatient Reconciliation: ERNI vs Snowflake'
    $summarySheet.Range('A1').Font.Size = 20
    $summarySheet.Range('A1').Font.Bold = $true
    $summarySheet.Range('A1').Font.Color = $white
    $summarySheet.Range('A1').Interior.Color = $navy
    $summarySheet.Range('A1').RowHeight = 34
    $summarySheet.Range('A3').Value2 = 'Scope'
    $summarySheet.Range('A3').Font.Bold = $true
    $summarySheet.Range('A4').Value2 = 'Providers'
    $summarySheet.Range('B4').Value2 = $providerDisplay
    $summarySheet.Range('A5').Value2 = 'ERNI source'
    $summarySheet.Range('B5').Value2 = '[dmic_sus_pbrmart].[dbo].[consolidated_ns_op]'
    $summarySheet.Range('A6').Value2 = 'Snowflake source'
    $summarySheet.Range('B6').Value2 = 'DEV__REPORTING.COMMISSIONING_REPORTING.FCT_SUS_OP_CONSOLIDATED'
    $summarySheet.Range('A7').Value2 = 'Percentage formula'
    $summarySheet.Range('B7').Value2 = '(Snowflake - ERNI) / ERNI'
    $summarySheet.Range('A8').Value2 = 'Reporting note'
    $summarySheet.Range('B8').Value2 = 'Coverage and refresh timing must be confirmed for partial years.'

    $summarySheet.Range('A10:F10').Merge()
    $summarySheet.Range('A10').Value2 = 'Key findings'
    $summarySheet.Range('A10').Font.Bold = $true
    $summarySheet.Range('A10').Font.Color = $white
    $summarySheet.Range('A10').Interior.Color = $blue
    $findingStartRow = 11
    for ($i=0; $i -lt $findings.Count; $i++) {
        $summarySheet.Cells.Item($findingStartRow + $i, 1).Value2 = [string][char]0x2022
        $summarySheet.Cells.Item($findingStartRow + $i, 2).Value2 = $findings[$i]
    }
    $findingEndRow = $findingStartRow + $findings.Count - 1
    $summarySheet.Range("B${findingStartRow}:B${findingEndRow}").WrapText = $true

    $stepsHeaderRow = $findingEndRow + 2
    $summarySheet.Range("A${stepsHeaderRow}:F${stepsHeaderRow}").Merge()
    $summarySheet.Cells.Item($stepsHeaderRow, 1).Value2 = 'Recommended next steps'
    $summarySheet.Cells.Item($stepsHeaderRow, 1).Font.Bold = $true
    $summarySheet.Cells.Item($stepsHeaderRow, 1).Font.Color = $white
    $summarySheet.Cells.Item($stepsHeaderRow, 1).Interior.Color = $blue
    $stepStartRow = $stepsHeaderRow + 1
    for ($i=0; $i -lt $steps.Count; $i++) {
        $summarySheet.Cells.Item($stepStartRow + $i, 1).Value2 = [string]($i + 1)
        $summarySheet.Cells.Item($stepStartRow + $i, 2).Value2 = $steps[$i]
    }
    $stepEndRow = $stepStartRow + $steps.Count - 1
    $summarySheet.Range("B${stepStartRow}:B${stepEndRow}").WrapText = $true
    $summarySheet.Columns.Item('A').ColumnWidth = 18
    $summarySheet.Columns.Item('B').ColumnWidth = 95
    $summarySheet.Columns.Item('C:F').ColumnWidth = 12
    $summarySheet.Range("A1:F${stepEndRow}").Font.Name = 'Aptos'
    $summarySheet.Range("A1:F${stepEndRow}").VerticalAlignment = -4160

    $headers = @('Financial year','ERNI rows','Snowflake rows','Difference','Difference %')
    for ($c=0; $c -lt $headers.Count; $c++) { $yearSheet.Cells.Item(1,$c+1).Value2 = $headers[$c] }
    for ($r=0; $r -lt $yearSummary.Count; $r++) {
        $item = $yearSummary[$r]
        $yearSheet.Cells.Item($r+2,1).Value2 = $item.FinancialYear
        $yearSheet.Cells.Item($r+2,2).Formula = '=' + ([double]$item.ERNI).ToString([Globalization.CultureInfo]::InvariantCulture)
        $yearSheet.Cells.Item($r+2,3).Formula = '=' + ([double]$item.Snowflake).ToString([Globalization.CultureInfo]::InvariantCulture)
        $yearSheet.Cells.Item($r+2,4).Formula = '=' + ([double]$item.Difference).ToString([Globalization.CultureInfo]::InvariantCulture)
        if ($null -ne $item.DifferencePct) {
            $yearSheet.Cells.Item($r+2,5).Formula = '=' + ([double]$item.DifferencePct).ToString([Globalization.CultureInfo]::InvariantCulture)
        }
    }
    $yearLastRow = $yearSummary.Count + 1
    $yearHeader = $yearSheet.Range("A1:E1")
    $yearHeader.Font.Bold = $true
    $yearHeader.Font.Color = $white
    $yearHeader.Interior.Color = $navy
    $yearSheet.Range("B2:D$yearLastRow").NumberFormat = '#,##0;[Red]-#,##0'
    $yearSheet.Range("E2:E$yearLastRow").NumberFormat = '0.00%;[Red]-0.00%'
    $yearSheet.Range("A1:E$yearLastRow").Borders.LineStyle = 1
    $yearSheet.Range("A1:E$yearLastRow").AutoFilter() | Out-Null
    $yearSheet.Activate()
    $yearSheet.Application.ActiveWindow.SplitRow = 1
    $yearSheet.Application.ActiveWindow.FreezePanes = $true
    $yearSheet.Columns.AutoFit() | Out-Null
    $yearSheet.Columns.Item('A').ColumnWidth = 16
    $yearSheet.Range("E2:E$yearLastRow").FormatConditions.AddColorScale(3) | Out-Null

    $chartObject = $yearSheet.ChartObjects().Add(430, 20, 590, 320)
    $chart = $chartObject.Chart
    $chart.ChartType = 4
    $chart.SetSourceData($yearSheet.Range("A1:A$yearLastRow,E1:E$yearLastRow"))
    $chart.HasTitle = $true
    $chart.ChartTitle.Text = 'Combined percentage difference by financial year'
    $chart.Axes(2).TickLabels.NumberFormat = '0%'

    $detailHeaders = @('Financial year','Provider','ERNI rows','Snowflake rows','Difference','Difference %','Review status')
    for ($c=0; $c -lt $detailHeaders.Count; $c++) { $detailSheet.Cells.Item(1,$c+1).Value2 = $detailHeaders[$c] }
    $detailRowNumber = 2
    foreach ($item in ($detailRows | Sort-Object FinancialYear,Provider)) {
        $detailSheet.Cells.Item($detailRowNumber,1).Value2 = $yearLabels[$item.FinancialYear]
        $detailSheet.Cells.Item($detailRowNumber,2).Value2 = $item.Provider
        $detailSheet.Cells.Item($detailRowNumber,3).Formula = '=' + ([double]$item.ERNI).ToString([Globalization.CultureInfo]::InvariantCulture)
        $detailSheet.Cells.Item($detailRowNumber,4).Formula = '=' + ([double]$item.Snowflake).ToString([Globalization.CultureInfo]::InvariantCulture)
        $detailSheet.Cells.Item($detailRowNumber,5).Formula = '=' + ([double]$item.Difference).ToString([Globalization.CultureInfo]::InvariantCulture)
        if ($null -ne $item.DifferencePct) {
            $detailSheet.Cells.Item($detailRowNumber,6).Formula = '=' + ([double]$item.DifferencePct).ToString([Globalization.CultureInfo]::InvariantCulture)
        }
        $status = if ($null -eq $item.DifferencePct) {
            'Investigate'
        } else {
            $absPct = [math]::Abs($item.DifferencePct)
            if ($absPct -le 0.005) { 'Close match' } elseif ($absPct -le 0.02) { 'Review' } else { 'Investigate' }
        }
        $detailSheet.Cells.Item($detailRowNumber,7).Value2 = $status
        $detailSheet.Cells.Item($detailRowNumber,7).Interior.Color = if ($status -eq 'Close match') { $lightGreen } elseif ($status -eq 'Review') { $lightAmber } else { $lightRed }
        $detailRowNumber++
    }
    $detailLastRow = $detailRowNumber - 1
    $detailHeader = $detailSheet.Range('A1:G1')
    $detailHeader.Font.Bold = $true
    $detailHeader.Font.Color = $white
    $detailHeader.Interior.Color = $navy
    $detailSheet.Range("C2:E$detailLastRow").NumberFormat = '#,##0;[Red]-#,##0'
    $detailSheet.Range("F2:F$detailLastRow").NumberFormat = '0.00%;[Red]-0.00%'
    $detailSheet.Range("A1:G$detailLastRow").Borders.LineStyle = 1
    $detailSheet.Range("A1:G$detailLastRow").AutoFilter() | Out-Null
    $detailSheet.Activate()
    $detailSheet.Application.ActiveWindow.SplitRow = 1
    $detailSheet.Application.ActiveWindow.FreezePanes = $true
    $detailSheet.Columns.AutoFit() | Out-Null
    $detailSheet.Columns.Item('A').ColumnWidth = 16
    $detailSheet.Columns.Item('G').ColumnWidth = 16

    $sqlSheet.Range('A1').Value2 = 'SQL used for reconciliation'
    $sqlSheet.Range('A1').Font.Size = 18
    $sqlSheet.Range('A1').Font.Bold = $true
    $sqlSheet.Range('A1').Font.Color = $white
    $sqlSheet.Range('A1').Interior.Color = $navy
    $sqlSheet.Range('A3').Value2 = 'ERNI SQL'
    $sqlSheet.Range('A3').Font.Bold = $true
    $sqlSheet.Range('A4').Value2 = $sqlErni
    $sqlSheet.Range('A6').Value2 = 'Snowflake SQL'
    $sqlSheet.Range('A6').Font.Bold = $true
    $sqlSheet.Range('A7').Value2 = $sqlSnowflake
    $sqlSheet.Range('A9').Value2 = 'Snowflake publication filters'
    $sqlSheet.Range('A9').Font.Bold = $true
    $sqlSheet.Range('A10').Value2 = "WHERE zcommissioning_access IN (0, 1)`n  AND COALESCE(administrative_category, '00') NOT IN ('02', '2')"
    $sqlSheet.Range('A4,A7,A10').Font.Name = 'Consolas'
    $sqlSheet.Range('A4,A7,A10').WrapText = $true
    $sqlSheet.Columns.Item('A').ColumnWidth = 120
    $sqlSheet.Rows.Item(4).RowHeight = 150
    $sqlSheet.Rows.Item(7).RowHeight = 150
    $sqlSheet.Rows.Item(10).RowHeight = 55

    foreach ($sheet in @($summarySheet,$yearSheet,$detailSheet,$sqlSheet)) {
        $sheet.Cells.Font.Name = 'Aptos'
        $sheet.Cells.VerticalAlignment = -4160
    }
    $sqlSheet.Range('A4,A7,A10').Font.Name = 'Consolas'

    $summarySheet.Activate()
    $workbook.SaveAs($xlsxPath, 51)
    $workbook.Close($true)
}
finally {
    if ($excel) {
        $excel.Quit()
        [System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel) | Out-Null
    }
    [gc]::Collect()
    [gc]::WaitForPendingFinalizers()
}

$subject = 'SUS OP reconciliation - all-year report for review'
$recipientDisplay = $Recipients -join ', '
$providerDisplay = @($detailRows.Provider | Sort-Object -Unique) -join ', '
$findingItems = foreach ($item in $yearSummary) {
    if ($null -eq $item.DifferencePct) {
        $status = 'Investigate: ERNI has zero rows'
        $percentage = 'n/a'
    } else {
        $absPct = [math]::Abs($item.DifferencePct)
        $status = if ($absPct -le 0.005) { 'Close match' } elseif ($absPct -le 0.02) { 'Review' } else { 'Investigate' }
        $percentage = $item.DifferencePct.ToString('P2')
    }
    "<li>$($item.FinancialYear): <b>$status</b>; difference $($item.Difference.ToString('N0')) ($percentage).</li>"
}
$findingsHtml = $findingItems -join "`n"

$htmlBody = @"
<html><body style="font-family:Calibri,Arial,sans-serif;font-size:11pt;color:#222">
<p>Hi $recipientDisplay,</p>
<p>I have completed an all-year reconciliation for these outpatient providers: <b>$providerDisplay</b>.</p>
<p>The attached Excel report contains:</p>
<ul>
<li>an executive summary and recommended next steps;</li>
<li>combined year-level results;</li>
<li>provider-level counts, differences and percentages for every comparable year;</li>
<li>the ERNI and Snowflake SQL used; and</li>
<li>the filters applied by the Snowflake consolidated model.</li>
</ul>
<p><b>Key findings</b></p>
<ul>
$findingsHtml
</ul>
<p><b>Review requested</b></p>
<p>Could you please review the attached output and confirm:</p>
<ol>
<li>whether the comparison periods have equivalent source coverage;</li>
<li>whether material differences have known dissemination or processing causes;</li>
<li>whether the comparison filters and provider scope are correct; and</li>
<li>whether we should proceed with month-level and record-level investigation.</li>
</ol>
<p>Subject to your feedback, the next step will be to investigate years marked Review or Investigate by financial month and identifier.</p>
<p>Kind regards,<br>$Sender</p>
</body></html>
"@

$boundary = '----=_Codex_' + [guid]::NewGuid().ToString('N')
$htmlBytes = [Text.Encoding]::UTF8.GetBytes($htmlBody)
$htmlBase64 = [Convert]::ToBase64String($htmlBytes, [Base64FormattingOptions]::InsertLineBreaks)
$attachmentBytes = [IO.File]::ReadAllBytes($xlsxPath)
$attachmentBase64 = [Convert]::ToBase64String($attachmentBytes, [Base64FormattingOptions]::InsertLineBreaks)
$dateHeader = [DateTimeOffset]::Now.ToString('r')
$eml = @"
From: $Sender
To: $($Recipients -join '; ')
Subject: $subject
Date: $dateHeader
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="$boundary"

--$boundary
Content-Type: text/html; charset="utf-8"
Content-Transfer-Encoding: base64

$htmlBase64
--$boundary
Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet; name="SUS_OP_ERNI_vs_Snowflake_Reconciliation.xlsx"
Content-Transfer-Encoding: base64
Content-Disposition: attachment; filename="SUS_OP_ERNI_vs_Snowflake_Reconciliation.xlsx"

$attachmentBase64
--$boundary--
"@ -replace "(?<!`r)`n", "`r`n"
[IO.File]::WriteAllText($emlPath, $eml, [Text.UTF8Encoding]::new($false))

Get-Item -LiteralPath $xlsxPath, $emlPath | Select-Object FullName, Length, LastWriteTime
