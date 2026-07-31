param(
    [string]$OutputDirectory = (Join-Path $PSScriptRoot '..\..\reports\sus_op_reconciliation')
)

$ErrorActionPreference = 'Stop'
$outputPath = [System.IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null

$xlsxPath = Join-Path $outputPath 'SUS_OP_ERNI_vs_Snowflake_Reconciliation.xlsx'
$emlPath = Join-Path $outputPath 'SUS_OP_Reconciliation_Review.eml'

$detailText = @'
1516|R1K|659913|99274
1516|RAS|308338|34818
1516|RQM|523547|254790
1516|RYJ|1064579|331777
1617|R1K|740234|103904
1617|RAS|384908|36498
1617|RQM|920523|381673
1617|RYJ|1240576|337147
1718|R1K|771295|771296
1718|RAS|413555|413555
1718|RQM|1007193|1007198
1718|RYJ|1499757|1499758
1819|R1K|775637|816452
1819|RAS|389934|413608
1819|RQM|849122|1057965
1819|RYJ|1359700|1557553
1920|R1K|844710|844707
1920|RAS|402948|403018
1920|RQM|1109701|1109692
1920|RYJ|1674369|1674364
2021|R1K|662474|668720
2021|RAS|308006|308499
2021|RQM|984580|998813
2021|RYJ|1495531|1518548
2122|R1K|868571|875370
2122|RAS|383772|385678
2122|RQM|1210918|1239958
2122|RYJ|1754297|1765503
2223|R1K|881451|884726
2223|RAS|384121|385375
2223|RQM|1225619|1229323
2223|RYJ|1753148|1737533
2324|R1K|1060290|1096770
2324|RAS|500250|509758
2324|RQM|1166729|1280900
2324|RYJ|1805282|1843778
2425|R1K|1457410|1478081
2425|RAS|665213|667988
2425|RQM|1388414|1388255
2425|RYJ|1956187|1957525
2526|R1K|1573887|1576315
2526|RAS|646622|648129
2526|RQM|1382143|1383452
2526|RYJ|1995745|1998787
2627|R1K|255784|256225
2627|RAS|103550|103725
2627|RQM|227140|227257
2627|RYJ|320563|321364
'@

$detailRows = foreach ($line in $detailText -split "`n") {
    if (-not $line.Trim()) { continue }
    $parts = $line.Trim() -split '\|'
    $erni = [long]$parts[2]
    $snowflake = [long]$parts[3]
    [pscustomobject]@{
        FinancialYear = $parts[0]
        Provider = $parts[1]
        ERNI = $erni
        Snowflake = $snowflake
        Difference = $snowflake - $erni
        DifferencePct = if ($erni) { ($snowflake - $erni) / $erni } else { $null }
    }
}

$yearLabels = @{
    '1516'='2015/16'; '1617'='2016/17'; '1718'='2017/18'; '1819'='2018/19'
    '1920'='2019/20'; '2021'='2020/21'; '2122'='2021/22'; '2223'='2022/23'
    '2324'='2023/24'; '2425'='2024/25'; '2526'='2025/26'; '2627'='2026/27*'
}

$yearSummary = foreach ($year in @('1516','1617','1718','1819','1920','2021','2122','2223','2324','2425','2526','2627')) {
    $rows = $detailRows | Where-Object FinancialYear -eq $year
    $erni = ($rows | Measure-Object ERNI -Sum).Sum
    $snowflake = ($rows | Measure-Object Snowflake -Sum).Sum
    [pscustomobject]@{
        FinancialYear = $yearLabels[$year]
        ERNI = $erni
        Snowflake = $snowflake
        Difference = $snowflake - $erni
        DifferencePct = ($snowflake - $erni) / $erni
    }
}

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
    $lightBlue = 0xF3E5D3
    $lightRed = 0xCEEBFF
    $lightAmber = 0xCCF2FF
    $lightGreen = 0xE2F0D9
    $white = 0xFFFFFF
    $darkText = 0x333333

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
    $summarySheet.Range('B4').Value2 = 'RYJ, RQM, RAS and R1K'
    $summarySheet.Range('A5').Value2 = 'ERNI source'
    $summarySheet.Range('B5').Value2 = '[dmic_sus_pbrmart].[dbo].[consolidated_ns_op]'
    $summarySheet.Range('A6').Value2 = 'Snowflake source'
    $summarySheet.Range('B6').Value2 = 'DEV__REPORTING.COMMISSIONING_REPORTING.FCT_SUS_OP_CONSOLIDATED'
    $summarySheet.Range('A7').Value2 = 'Percentage formula'
    $summarySheet.Range('B7').Value2 = '(Snowflake - ERNI) / ERNI'
    $summarySheet.Range('A8').Value2 = 'Reporting note'
    $summarySheet.Range('B8').Value2 = '2026/27 is a partial year.'

    $summarySheet.Range('A10:F10').Merge()
    $summarySheet.Range('A10').Value2 = 'Key findings'
    $summarySheet.Range('A10').Font.Bold = $true
    $summarySheet.Range('A10').Font.Color = $white
    $summarySheet.Range('A10').Interior.Color = $blue
    $findings = @(
        '2015/16 and 2016/17 are materially incomplete in Snowflake and require historical coverage confirmation.',
        'The unbundled-HRG join was fixed; all four main providers now have zero duplicate encounter rows.',
        '2017/18 and 2019/20 reconcile almost exactly across the four main providers.',
        '2018/19 is a material exception (+13.97%), led by RQM and RYJ.',
        '2023/24 is the main recent exception (+4.38%), led by RQM (+9.79%).',
        '2024/25, 2025/26 and partial 2026/27 reconcile within 0.5% in total.'
    )
    for ($i=0; $i -lt $findings.Count; $i++) {
        $summarySheet.Cells.Item(11 + $i, 1).Value2 = [char]0x2022
        $summarySheet.Cells.Item(11 + $i, 2).Value2 = $findings[$i]
    }
    $summarySheet.Range('B11:B16').WrapText = $true

    $summarySheet.Range('A18:F18').Merge()
    $summarySheet.Range('A18').Value2 = 'Recommended next steps'
    $summarySheet.Range('A18').Font.Bold = $true
    $summarySheet.Range('A18').Font.Color = $white
    $summarySheet.Range('A18').Interior.Color = $blue
    $steps = @(
        'Confirm expected Snowflake historical coverage for 2015/16 and 2016/17.',
        'Break down 2018/19 and 2023/24 by financial month.',
        'Prioritise RQM and RYJ for encounter- and attendance-level comparison.',
        'Compare total rows with distinct SK encounter IDs and attendance identifiers.',
        'Use an exactly matching month as a control during record-level reconciliation.'
    )
    for ($i=0; $i -lt $steps.Count; $i++) {
        $summarySheet.Cells.Item(19 + $i, 1).Value2 = [string]($i + 1)
        $summarySheet.Cells.Item(19 + $i, 2).Value2 = $steps[$i]
    }
    $summarySheet.Range('B19:B23').WrapText = $true
    $summarySheet.Columns.Item('A').ColumnWidth = 18
    $summarySheet.Columns.Item('B').ColumnWidth = 95
    $summarySheet.Columns.Item('C:F').ColumnWidth = 12
    $summarySheet.Range('A1:F23').Font.Name = 'Aptos'
    $summarySheet.Range('A1:F23').VerticalAlignment = -4160

    $headers = @('Financial year','ERNI rows','Snowflake rows','Difference','Difference %')
    for ($c=0; $c -lt $headers.Count; $c++) { $yearSheet.Cells.Item(1,$c+1).Value2 = $headers[$c] }
    for ($r=0; $r -lt $yearSummary.Count; $r++) {
        $item = $yearSummary[$r]
        $yearSheet.Cells.Item($r+2,1).Value2 = $item.FinancialYear
        $yearSheet.Cells.Item($r+2,2).Formula = '=' + ([double]$item.ERNI).ToString([Globalization.CultureInfo]::InvariantCulture)
        $yearSheet.Cells.Item($r+2,3).Formula = '=' + ([double]$item.Snowflake).ToString([Globalization.CultureInfo]::InvariantCulture)
        $yearSheet.Cells.Item($r+2,4).Formula = '=' + ([double]$item.Difference).ToString([Globalization.CultureInfo]::InvariantCulture)
        $yearSheet.Cells.Item($r+2,5).Formula = '=' + ([double]$item.DifferencePct).ToString([Globalization.CultureInfo]::InvariantCulture)
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
        $detailSheet.Cells.Item($detailRowNumber,6).Formula = '=' + ([double]$item.DifferencePct).ToString([Globalization.CultureInfo]::InvariantCulture)
        $absPct = [math]::Abs($item.DifferencePct)
        $status = if ($absPct -le 0.005) { 'Close match' } elseif ($absPct -le 0.02) { 'Review' } else { 'Investigate' }
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
$htmlBody = @'
<html><body style="font-family:Calibri,Arial,sans-serif;font-size:11pt;color:#222">
<p>Hi Janni, Nathan and Ibby,</p>
<p>Following Ibby's feedback, I have completed an all-year reconciliation focused on our four main outpatient providers: <b>RYJ, RQM, RAS and R1K</b>.</p>
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
<li>2015/16 and 2016/17 appear materially incomplete in Snowflake and need historical coverage confirmation.</li>
<li>The Snowflake unbundled-HRG join has been fixed and there are now zero duplicate encounter rows for the four main providers.</li>
<li>2017/18 and 2019/20 reconcile almost exactly.</li>
<li>2018/19 is a material exception (+13.97%), mainly driven by RQM and RYJ.</li>
<li>2023/24 is the main recent exception (+4.38%), with RQM at +9.79%.</li>
<li>2024/25, 2025/26 and partial 2026/27 reconcile within 0.5% overall.</li>
<li>The remaining 2026/27 difference is 1,534 rows (0.17%) and is consistent with refresh timing between ERNI CURRENT and the Snowflake Date Range extract.</li>
</ul>
<p><b>Review requested</b></p>
<p>Could you please review the attached output and confirm:</p>
<ol>
<li>whether Snowflake is expected to contain complete data for 2015/16 and 2016/17;</li>
<li>whether 2018/19 and 2023/24 had any known dissemination or source-processing differences;</li>
<li>whether the comparison filters and provider scope are correct; and</li>
<li>whether we should proceed with a month-level and record-level investigation of RQM and RYJ.</li>
</ol>
<p>Subject to your feedback, the next step will be to break down 2018/19 and 2023/24 by financial month and compare distinct encounter and attendance identifiers.</p>
<p>Kind regards,<br>Dharmesh</p>
</body></html>
'@

$boundary = '----=_Codex_' + [guid]::NewGuid().ToString('N')
$htmlBytes = [Text.Encoding]::UTF8.GetBytes($htmlBody)
$htmlBase64 = [Convert]::ToBase64String($htmlBytes, [Base64FormattingOptions]::InsertLineBreaks)
$attachmentBytes = [IO.File]::ReadAllBytes($xlsxPath)
$attachmentBase64 = [Convert]::ToBase64String($attachmentBytes, [Base64FormattingOptions]::InsertLineBreaks)
$dateHeader = [DateTimeOffset]::Now.ToString('r')
$eml = @"
From: Dharmesh
To: Janni; Nathan; Ibby
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
