"""Generate seeds/sds_role_groups.csv from the NHS Digital SDS Role Groups table.

Source: https://digital.nhs.uk/data-and-information/publications/statistical/
        appointments-in-general-practice/sds-role-groups

The raw table below is pasted verbatim from the NHS Digital HTML table.
Re-run this script to regenerate the seed if SDS publishes an updated mapping.

Classification notes:
- sds_role_group is the official NHS Digital 4-group (+ Unknown) classification.
- practitioner_role_group is our analytical grouping, richer than SDS to preserve
  ARRS / workforce-mix signal that matters for primary care reporting.
- is_arrs_role is TRUE only for codes whose SDS description is explicitly an
  ARRS-scheme role (R9801, R9803, R9804, R9806-R9808, R9811-R9813, R9815).
  Generic role codes (R1290 Pharmacist, R1070 Paramedic) are FALSE because
  non-ARRS staff can hold them; use the seed + practice funding data together
  if you need a precise ARRS headcount.
"""

from pathlib import Path

# (role_code, role_description, sds_role_group)
RAW = [
    # --- GP ---
    ("R0110", "Specialist Registrar", "GP"),
    ("R0120", "Senior Registrar (Closed)", "GP"),
    ("R0130", "Registrar (Closed)", "GP"),
    ("R0190", "Trust Grade Doctor - Specialist Registrar level", "GP"),
    ("R0215", "Asst. Clinical Medical Officer", "GP"),
    ("R0220", "Clinical Medical Officer", "GP"),
    ("R0230", "Senior Clinical Medical Officer", "GP"),
    ("R0260", "General Medical Practitioner", "GP"),
    ("R0261", "Assistant GP", "GP"),
    ("R0262", "Locum GP", "GP"),
    ("R0263", "Deputising Doctor", "GP"),
    ("R0270", "Salaried General Practitioner", "GP"),
    ("R1547", "Associate Practitioner - General Practitioner", "GP"),
    ("R6200", "GP Registrar", "GP"),
    ("R6300", "Sessional GP", "GP"),
    ("R8000", "Clinical Practitioner Access Role", "GP"),
    ("R9001", "OBSOLETE EBS GP (Referrer)", "GP"),
    # --- Nurses ---
    ("E1002", "Health Visitor", "Nurses"),
    ("R0006", "Student Community Practitioner", "Nurses"),
    ("R0330", "Student Nurse - Adult Branch", "Nurses"),
    ("R0340", "Student Nurse - Child Branch", "Nurses"),
    ("R0350", "Student Nurse - Mental Health Branch", "Nurses"),
    ("R0360", "Student Nurse - Learning Disabilities Branch", "Nurses"),
    ("R0390", "Student District Nurse", "Nurses"),
    ("R0400", "Student School Nurse", "Nurses"),
    ("R0410", "Student Practice Nurse", "Nurses"),
    ("R0420", "Student Occupational Health Nurse", "Nurses"),
    ("R0430", "Student Community Paediatric Nurse", "Nurses"),
    ("R0440", "Student Community Mental Health Nurse", "Nurses"),
    ("R0450", "Student Community Learning Disabilities Nurse", "Nurses"),
    ("R0560", "Director of Nursing", "Nurses"),
    ("R0570", "Nurse Consultant", "Nurses"),
    ("R0580", "Nurse Manager", "Nurses"),
    ("R0590", "Modern Matron", "Nurses"),
    ("R0600", "Specialist Nurse Practitioner", "Nurses"),
    ("R0610", "Sister/Charge Nurse", "Nurses"),
    ("R0620", "Staff Nurse", "Nurses"),
    ("R0630", "Enrolled Nurse", "Nurses"),
    ("R0690", "Community Practitioner", "Nurses"),
    ("R0700", "Community Nurse", "Nurses"),
    ("R1490", "Nursery Nurse", "Nurses"),
    ("R1543", "Associate Practitioner - Nurse", "Nurses"),
    ("R1640", "Nursing Cadet", "Nurses"),
    ("R1974", "Community Learning Disabilities Nurse", "Nurses"),
    ("R1975", "Community Mental Health Nurse", "Nurses"),
    ("R1976", "Community Team Manager", "Nurses"),
    ("R8001", "Nurse Access Role", "Nurses"),
    ("R8002", "Nurse Manager Access Role", "Nurses"),
    ("R9809", "Nursing Associates", "Nurses"),
    ("R9816", "Trainee Nursing Associates", "Nurses"),
    # --- Other Direct Patient Care ---
    ("E1001", "Advanced Practitioner", "Other Direct Patient Care"),
    ("E1003", "Physician Assistant", "Other Direct Patient Care"),
    ("E1006", "Healthcare Science Practitioner", "Other Direct Patient Care"),
    ("E1007", "Healthcare Science Associate", "Other Direct Patient Care"),
    ("E1008", "Healthcare Science Assistant", "Other Direct Patient Care"),
    ("R0002", "Porter", "Other Direct Patient Care"),
    ("R0003", "Clinical Application Administrator", "Other Direct Patient Care"),
    ("R0010", "Medical Director", "Other Direct Patient Care"),
    ("R0011", "Dispenser", "Other Direct Patient Care"),
    ("R0012", "Radiographer", "Other Direct Patient Care"),
    ("R0013", "Student Radiographer", "Other Direct Patient Care"),
    ("R0014", "Radiologist", "Other Direct Patient Care"),
    ("R0015", "PACS Administrator", "Other Direct Patient Care"),
    ("R0016", "Reporting Radiographer", "Other Direct Patient Care"),
    ("R0017", "Assistant Practitioner", "Other Direct Patient Care"),
    ("R0018", "Audiologist", "Other Direct Patient Care"),
    ("R0019", "Medical Technical Officer", "Other Direct Patient Care"),
    ("R0020", "Clinical Director - Medical", "Other Direct Patient Care"),
    ("R0030", "Professor", "Other Direct Patient Care"),
    ("R0040", "Senior Lecturer", "Other Direct Patient Care"),
    ("R0050", "Consultant", "Other Direct Patient Care"),
    ("R0055", "Dental surgeon acting as Hospital Consultant", "Other Direct Patient Care"),
    ("R0060", "Special salary scale in Public Health Medicine", "Other Direct Patient Care"),
    ("R0070", "Associate Specialist", "Other Direct Patient Care"),
    ("R0080", "Staff Grade", "Other Direct Patient Care"),
    ("R0090", "Hospital Practitioner", "Other Direct Patient Care"),
    ("R0100", "Clinical Assistant", "Other Direct Patient Care"),
    ("R0140", "Senior House Officer", "Other Direct Patient Care"),
    ("R0150", "House Officer - Pre Registration", "Other Direct Patient Care"),
    ("R0160", "House Officer - Post Registration", "Other Direct Patient Care"),
    ("R0170", "Trust Grade Doctor - House Officer level", "Other Direct Patient Care"),
    ("R0180", "Trust Grade Doctor - SHO level", "Other Direct Patient Care"),
    ("R0200", "Trust Grade Doctor - Career Grade level", "Other Direct Patient Care"),
    ("R0210", "Director of Public Health", "Other Direct Patient Care"),
    ("R0240", "Other Community Health Service", "Other Direct Patient Care"),
    ("R0243", "Other Community Health Service - Social Care Worker", "Other Direct Patient Care"),
    ("R0247", "Other Community Health Service - Admin Clerk", "Other Direct Patient Care"),
    ("R0250", "General Dental Practitioner", "Other Direct Patient Care"),
    ("R0280", "Regional Dental Officer", "Other Direct Patient Care"),
    ("R0290", "Dental Clinical Director - Dental", "Other Direct Patient Care"),
    ("R0295", "Dental Assistant Clinical Director", "Other Direct Patient Care"),
    ("R0300", "Dental Officer", "Other Direct Patient Care"),
    ("R0310", "Senior Dental Officer", "Other Direct Patient Care"),
    ("R0320", "Salaried Dental Practitioner", "Other Direct Patient Care"),
    ("R0370", "Student Midwife", "Other Direct Patient Care"),
    ("R0380", "Student Health Visitor", "Other Direct Patient Care"),
    ("R0460", "Student Chiropodist", "Other Direct Patient Care"),
    ("R0470", "Student Dietitian", "Other Direct Patient Care"),
    ("R0480", "Student Occupational Therapist", "Other Direct Patient Care"),
    ("R0490", "Student Orthoptist", "Other Direct Patient Care"),
    ("R0500", "Student Physiotherapist", "Other Direct Patient Care"),
    ("R0510", "Student Radiographer - Diagnostic", "Other Direct Patient Care"),
    ("R0520", "Student Radiographer - Therapeutic", "Other Direct Patient Care"),
    ("R0530", "Student Speech & Language Therapist", "Other Direct Patient Care"),
    ("R0540", "Art, Music & Drama Student", "Other Direct Patient Care"),
    ("R0550", "Student Psychotherapist", "Other Direct Patient Care"),
    ("R0640", "Midwife - Consultant", "Other Direct Patient Care"),
    ("R0650", "Midwife - Specialist Practitioner", "Other Direct Patient Care"),
    ("R0660", "Midwife - Manager", "Other Direct Patient Care"),
    ("R0670", "Midwife - Sister/Charge Nurse", "Other Direct Patient Care"),
    ("R0680", "Midwife", "Other Direct Patient Care"),
    ("R0710", "Art Therapist", "Other Direct Patient Care"),
    ("R0720", "Art Therapist Consultant", "Other Direct Patient Care"),
    ("R0730", "Art Therapist Manager", "Other Direct Patient Care"),
    ("R0740", "Art Therapist Specialist Practitioner", "Other Direct Patient Care"),
    ("R0750", "Chiropodist/Podiatrist", "Other Direct Patient Care"),
    ("R0760", "Chiropodist/Podiatrist Consultant", "Other Direct Patient Care"),
    ("R0770", "Chiropodist/Podiatrist Manager", "Other Direct Patient Care"),
    ("R0780", "Chiropodist/Podiatrist Specialist Practitioner", "Other Direct Patient Care"),
    ("R0790", "Dietitian", "Other Direct Patient Care"),
    ("R0800", "Dietitian Consultant", "Other Direct Patient Care"),
    ("R0810", "Dietitian Manager", "Other Direct Patient Care"),
    ("R0820", "Dietitian Specialist Practitioner", "Other Direct Patient Care"),
    ("R0830", "Drama Therapist", "Other Direct Patient Care"),
    ("R0840", "Drama Therapist Consultant", "Other Direct Patient Care"),
    ("R0850", "Drama Therapist Manager", "Other Direct Patient Care"),
    ("R0860", "Drama Therapist Specialist Practitioner", "Other Direct Patient Care"),
    ("R0870", "Multi Therapist", "Other Direct Patient Care"),
    ("R0880", "Multi Therapist Consultant", "Other Direct Patient Care"),
    ("R0890", "Multi Therapist Manager", "Other Direct Patient Care"),
    ("R0900", "Multi Therapist Specialist Practitioner", "Other Direct Patient Care"),
    ("R0910", "Music Therapist", "Other Direct Patient Care"),
    ("R0920", "Music Therapist Consultant", "Other Direct Patient Care"),
    ("R0930", "Music Therapist Manager", "Other Direct Patient Care"),
    ("R0940", "Music Therapist Specialist Practitioner", "Other Direct Patient Care"),
    ("R0950", "Occupational Therapist", "Other Direct Patient Care"),
    ("R0955", "Speech & Language Therapist", "Other Direct Patient Care"),
    ("R0960", "Occupational Therapist Consultant", "Other Direct Patient Care"),
    ("R0965", "Speech & Language Therapist Consultant", "Other Direct Patient Care"),
    ("R0970", "Occupational Therapist Manager", "Other Direct Patient Care"),
    ("R0975", "Speech & Language Therapist Manager", "Other Direct Patient Care"),
    ("R0980", "Occupational Therapy Specialist Practitioner", "Other Direct Patient Care"),
    ("R0985", "Speech & Language Therapist Specialist Practitioner", "Other Direct Patient Care"),
    ("R0990", "Orthoptist", "Other Direct Patient Care"),
    ("R1000", "Orthoptist Consultant", "Other Direct Patient Care"),
    ("R1010", "Orthoptist Manager", "Other Direct Patient Care"),
    ("R1020", "Orthoptist Specialist Practitioner", "Other Direct Patient Care"),
    ("R1030", "Orthotist", "Other Direct Patient Care"),
    ("R1040", "Orthotist Consultant", "Other Direct Patient Care"),
    ("R1050", "Orthotist Manager", "Other Direct Patient Care"),
    ("R1060", "Orthotist Specialist Practitioner", "Other Direct Patient Care"),
    ("R1070", "Paramedic", "Other Direct Patient Care"),
    ("R1080", "Paramedic Consultant", "Other Direct Patient Care"),
    ("R1090", "Paramedic Manager", "Other Direct Patient Care"),
    ("R1100", "Paramedic Specialist Practitioner", "Other Direct Patient Care"),
    ("R1110", "Physiotherapist", "Other Direct Patient Care"),
    ("R1120", "Physiotherapist Consultant", "Other Direct Patient Care"),
    ("R1130", "Physiotherapist Manager", "Other Direct Patient Care"),
    ("R1140", "Physiotherapist Specialist Practitioner", "Other Direct Patient Care"),
    ("R1150", "Prosthetist", "Other Direct Patient Care"),
    ("R1160", "Prosthetist Consultant", "Other Direct Patient Care"),
    ("R1170", "Prosthetist Manager", "Other Direct Patient Care"),
    ("R1180", "Prosthetist Specialist Practitioner", "Other Direct Patient Care"),
    ("R1190", "Radiographer - Diagnostic", "Other Direct Patient Care"),
    ("R1200", "Radiographer - Diagnostic, Consultant", "Other Direct Patient Care"),
    ("R1210", "Radiographer - Diagnostic, Manager", "Other Direct Patient Care"),
    ("R1220", "Radiographer - Diagnostic, Specialist Practitioner", "Other Direct Patient Care"),
    ("R1230", "Radiographer - Therapeutic", "Other Direct Patient Care"),
    ("R1240", "Radiographer - Therapeutic, Consultant", "Other Direct Patient Care"),
    ("R1250", "Radiographer - Therapeutic, Manager", "Other Direct Patient Care"),
    ("R1260", "Radiographer - Therapeutic, Specialist Practitioner", "Other Direct Patient Care"),
    ("R1280", "Optometrist", "Other Direct Patient Care"),
    ("R1290", "Pharmacist", "Other Direct Patient Care"),
    ("R1300", "Psychotherapist", "Other Direct Patient Care"),
    ("R1310", "Clinical Psychologist", "Other Direct Patient Care"),
    ("R1320", "Chaplain", "Other Direct Patient Care"),
    ("R1330", "Social Worker", "Other Direct Patient Care"),
    ("R1340", "Approved Social Worker", "Other Direct Patient Care"),
    ("R1350", "Youth Worker", "Other Direct Patient Care"),
    ("R1360", "Specialist Practitioner", "Other Direct Patient Care"),
    ("R1370", "Practitioner", "Other Direct Patient Care"),
    ("R1380", "Technician - PS&T", "Other Direct Patient Care"),
    ("R1390", "Osteopath", "Other Direct Patient Care"),
    ("R1400", "Healthcare Scientist", "Other Direct Patient Care"),
    ("R1410", "Consultant Healthcare Scientist", "Other Direct Patient Care"),
    ("R1420", "Biomedical Scientist", "Other Direct Patient Care"),
    ("R1430", "Technician - Healthcare Scientists", "Other Direct Patient Care"),
    ("R1440", "Therapist", "Other Direct Patient Care"),
    ("R1450", "Health Care Support Worker", "Other Direct Patient Care"),
    ("R1460", "Social Care Support Worker", "Other Direct Patient Care"),
    ("R1470", "Home Help", "Other Direct Patient Care"),
    ("R1480", "Healthcare Assistant", "Other Direct Patient Care"),
    ("R1500", "Play Therapist", "Other Direct Patient Care"),
    ("R1510", "Play Specialist", "Other Direct Patient Care"),
    ("R1520", "Technician - Add'l Clinical Services", "Other Direct Patient Care"),
    ("R1530", "Technical Instructor", "Other Direct Patient Care"),
    ("R1540", "Associate Practitioner", "Other Direct Patient Care"),
    ("R1550", "Counsellor", "Other Direct Patient Care"),
    ("R1560", "Helper/Assistant", "Other Direct Patient Care"),
    ("R1570", "Dental Surgery Assistant", "Other Direct Patient Care"),
    ("R1580", "Medical Laboratory Assistant", "Other Direct Patient Care"),
    ("R1590", "Phlebotomist", "Other Direct Patient Care"),
    ("R1600", "Cytoscreener", "Other Direct Patient Care"),
    ("R1610", "Student Technician", "Other Direct Patient Care"),
    ("R1620", "Trainee Scientist", "Other Direct Patient Care"),
    ("R1630", "Trainee Practitioner", "Other Direct Patient Care"),
    ("R1650", "Healthcare Cadet", "Other Direct Patient Care"),
    ("R1660", "Pre-reg Pharmacist", "Other Direct Patient Care"),
    ("R1670", "Assistant Psychologist", "Other Direct Patient Care"),
    ("R1680", "Assistant Psychotherapist", "Other Direct Patient Care"),
    ("R1979", "Medical Technical Officer - Pharmacy", "Other Direct Patient Care"),
    ("R1980", "Patient Welfare Officer", "Other Direct Patient Care"),
    ("R1981", "Psychiatrist", "Other Direct Patient Care"),
    ("R2500", "Support Worker", "Other Direct Patient Care"),
    ("R2570", "Technician", "Other Direct Patient Care"),
    ("R2600", "Assistant", "Other Direct Patient Care"),
    ("R2740", "Apprentice", "Other Direct Patient Care"),
    ("R5220", "Pharmacy Technician", "Other Direct Patient Care"),
    ("R5230", "Community Pharmacy Assistant", "Other Direct Patient Care"),
    ("R5240", "OBSOLETE Community Pharmacist", "Other Direct Patient Care"),
    ("R5260", "Clinical Pharmacy Specialist", "Other Direct Patient Care"),
    ("R6400", "Medical Student", "Other Direct Patient Care"),
    ("R7140", "ODP", "Other Direct Patient Care"),
    ("R7150", "SODP", "Other Direct Patient Care"),
    ("R8003", "Health Professional Access Role", "Other Direct Patient Care"),
    ("R8004", "Healthcare Student Access Role", "Other Direct Patient Care"),
    ("R8005", "Biomedical Scientist Access Role", "Other Direct Patient Care"),
    ("R8006", "Medical Secretary Access Role", "Other Direct Patient Care"),
    ("R8007", "Clinical Coder Access Role", "Other Direct Patient Care"),
    ("R8014", "Social Worker Access Role", "Other Direct Patient Care"),
    ("R8016", "Midwife Access Role", "Other Direct Patient Care"),
    ("R8017", "Midwife Manager Access Role", "Other Direct Patient Care"),
    ("R8024", "Bank Access Role", "Other Direct Patient Care"),
    ("R9005", "OBSOLETE EBS Clinician (Trust)", "Other Direct Patient Care"),
    ("R9006", "OBSOLETE EBS Clinician (BMS)", "Other Direct Patient Care"),
    ("R9100", "A & E Staff Nurse (Temporary) London Cluster Only", "Other Direct Patient Care"),
    ("R9101", "A & E Manager (Temporary) London Cluster Only", "Other Direct Patient Care"),
    ("R9102", "A & E Doctor (Temporary) London Cluster only", "Other Direct Patient Care"),
    ("R9103", "A & E Student (Temporary) London Cluster Only", "Other Direct Patient Care"),
    ("R9104", "A & E Clerk (Temporary) London Cluster Only", "Other Direct Patient Care"),
    ("R9500", "Social services senior management", "Other Direct Patient Care"),
    ("R9505", "Social services policy and planning", "Other Direct Patient Care"),
    ("R9510", "Social Services information manager", "Other Direct Patient Care"),
    ("R9515", "Social work team manager (children)", "Other Direct Patient Care"),
    ("R9520", "Senior social worker (children)", "Other Direct Patient Care"),
    ("R9525", "Social services care manager (children)", "Other Direct Patient Care"),
    ("R9530", "Social work assistant (children)", "Other Direct Patient Care"),
    ("R9535", "Child Protection worker", "Other Direct Patient Care"),
    ("R9540", "Family Placement worker", "Other Direct Patient Care"),
    ("R9545", "Community Worker (children)", "Other Direct Patient Care"),
    ("R9550", "Occupational therapist", "Other Direct Patient Care"),
    ("R9555", "OT assistant", "Other Direct Patient Care"),
    ("R9560", "Occupational Therapy Team Manager", "Other Direct Patient Care"),
    ("R9565", "Social work team manager (adults)", "Other Direct Patient Care"),
    ("R9570", "Senior social worker (adults)", "Other Direct Patient Care"),
    ("R9575", "Social services care manager (adults)", "Other Direct Patient Care"),
    ("R9580", "Social work assistant (adults)", "Other Direct Patient Care"),
    ("R9585", "Social work team manager (mental health)", "Other Direct Patient Care"),
    ("R9590", "Senior social worker (mental health)", "Other Direct Patient Care"),
    ("R9595", "Social services care manager (mental health)", "Other Direct Patient Care"),
    ("R9600", "Social work assistant (mental health)", "Other Direct Patient Care"),
    ("R9605", "Emergency Duty social worker", "Other Direct Patient Care"),
    ("R9615", "Social services driver", "Other Direct Patient Care"),
    ("R9620", "Home Care organiser", "Other Direct Patient Care"),
    ("R9625", "Home Care administrator", "Other Direct Patient Care"),
    ("R9630", "Home help", "Other Direct Patient Care"),
    ("R9635", "Warden", "Other Direct Patient Care"),
    ("R9640", "Meals on wheels organiser", "Other Direct Patient Care"),
    ("R9645", "Meals delivery", "Other Direct Patient Care"),
    ("R9650", "Day centre manager", "Other Direct Patient Care"),
    ("R9655", "Day centre deputy", "Other Direct Patient Care"),
    ("R9660", "Day Centre officer", "Other Direct Patient Care"),
    ("R9665", "Day centre care staff", "Other Direct Patient Care"),
    ("R9670", "Family centre manager", "Other Direct Patient Care"),
    ("R9675", "Family centre deputy", "Other Direct Patient Care"),
    ("R9680", "Family centre worker", "Other Direct Patient Care"),
    ("R9685", "Nursery manager", "Other Direct Patient Care"),
    ("R9690", "Nursery deputy", "Other Direct Patient Care"),
    ("R9695", "Nursery worker", "Other Direct Patient Care"),
    ("R9700", "Playgroup leader", "Other Direct Patient Care"),
    ("R9705", "Playgroup assistant", "Other Direct Patient Care"),
    ("R9710", "Residential manager", "Other Direct Patient Care"),
    ("R9715", "Residential deputy", "Other Direct Patient Care"),
    ("R9720", "Residential worker", "Other Direct Patient Care"),
    ("R9725", "Residential care staff", "Other Direct Patient Care"),
    ("R9730", "Intermediate Care Manager", "Other Direct Patient Care"),
    ("R9735", "Intermediate Care deputy", "Other Direct Patient Care"),
    ("R9740", "Intermediate Care worker", "Other Direct Patient Care"),
    ("R9745", "Intermediate Care staff", "Other Direct Patient Care"),
    ("R9750", "Social Care SAP User", "Other Direct Patient Care"),
    ("R9755", "Social Care SAP Manager", "Other Direct Patient Care"),
    ("R9802", "Chiropodist/Podiatrist Advanced Practitioner", "Other Direct Patient Care"),
    ("R9803", "Clinical Pharmacist Advanced Practitioner", "Other Direct Patient Care"),
    ("R9804", "Clinical Pharmacists", "Other Direct Patient Care"),
    ("R9805", "Dietitian Advanced Practitioner", "Other Direct Patient Care"),
    ("R9806", "First Contact Physiotherapists", "Other Direct Patient Care"),
    ("R9807", "Health and Wellbeing Coaches", "Other Direct Patient Care"),
    ("R9808", "Mental Health Practitioners", "Other Direct Patient Care"),
    ("R9810", "Occupational Therapist Advanced Practitioner", "Other Direct Patient Care"),
    ("R9811", "Paramedic Advanced Practitioner", "Other Direct Patient Care"),
    ("R9812", "Pharmacy Technicians", "Other Direct Patient Care"),
    ("R9813", "Physician Associates", "Other Direct Patient Care"),
    ("R9814", "Physiotherapist Advanced Practitioner", "Other Direct Patient Care"),
    ("R9815", "Social Prescribing Link Workers", "Other Direct Patient Care"),
    # --- Data Quality ---
    ("R0001", "Privacy Officer", "Data Quality"),
    ("R0007", "ERS SDS Organisation Reporting Analyst", "Data Quality"),
    ("R0008", "Demographic Supervisor", "Data Quality"),
    ("R0021", "DSA NHS Number Manager (TEMPORARY)", "Data Quality"),
    ("R0022", "DSA National Clinical Supervisor (TEMPORARY)", "Data Quality"),
    ("R0023", "DSA National Clinical Administrator (TEMPORARY)", "Data Quality"),
    ("R1270", "Clinical Director", "Data Quality"),
    ("R1690", "Call Operator", "Data Quality"),
    ("R1700", "Gateway Worker", "Data Quality"),
    ("R1710", "Support, Time, Recovery Worker", "Data Quality"),
    ("R1720", "Clerical Worker", "Data Quality"),
    ("R1730", "Receptionist", "Data Quality"),
    ("R1740", "Secretary", "Data Quality"),
    ("R1750", "Personal Assistant", "Data Quality"),
    ("R1751", "Demographic Administrator (Sensitive Records) Temporary", "Data Quality"),
    ("R1760", "Medical Secretary", "Data Quality"),
    ("R1770", "Officer", "Data Quality"),
    ("R1780", "Manager", "Data Quality"),
    ("R1790", "Senior Manager", "Data Quality"),
    ("R1800", "Technician - Admin & Clerical", "Data Quality"),
    ("R1810", "Accountant", "Data Quality"),
    ("R1820", "Librarian", "Data Quality"),
    ("R1830", "Interpreter", "Data Quality"),
    ("R1840", "Analyst", "Data Quality"),
    ("R1850", "Adviser", "Data Quality"),
    ("R1860", "Researcher", "Data Quality"),
    ("R1870", "Control Assistant", "Data Quality"),
    ("R1880", "Architect", "Data Quality"),
    ("R1890", "Lawyer", "Data Quality"),
    ("R1900", "Surveyor", "Data Quality"),
    ("R1910", "Chair", "Data Quality"),
    ("R1920", "Chief Executive", "Data Quality"),
    ("R1930", "Finance Director", "Data Quality"),
    ("R1940", "Other Executive Director", "Data Quality"),
    ("R1950", "Board Level Director", "Data Quality"),
    ("R1960", "Non Executive Director", "Data Quality"),
    ("R1970", "Childcare Co-ordinator", "Data Quality"),
    ("R1971", "Map of Medicine Administrator", "Data Quality"),
    ("R1972", "Clinical Team Manager", "Data Quality"),
    ("R1973", "Community Administrator", "Data Quality"),
    ("R1977", "ECC/CPA Administrator", "Data Quality"),
    ("R1978", "Information Officer", "Data Quality"),
    ("R1982", "Senior Administrator", "Data Quality"),
    ("R1983", "Ward Manager", "Data Quality"),
    ("R1984", "Health Records Administrator", "Data Quality"),
    ("R1985", "Health Records Clerk", "Data Quality"),
    ("R1986", "Workgroup Administrator", "Data Quality"),
    ("R1987", "National RBAC Attribute Administrator", "Data Quality"),
    ("R1988", "National RBAC Baseline Policy Administrator", "Data Quality"),
    ("R1989", "Complaints Coordinator", "Data Quality"),
    ("R1990", "Complaints Investigator", "Data Quality"),
    ("R1995", "End Point Approver", "Data Quality"),
    ("R1996", "End Point DNS Administrator", "Data Quality"),
    ("R1997", "End Point Spine Administrator", "Data Quality"),
    ("R1998", "End Point Super User", "Data Quality"),
    ("R1999", "End Point Service Administrator", "Data Quality"),
    ("R2510", "Housekeeper", "Data Quality"),
    ("R2520", "Cook", "Data Quality"),
    ("R2540", "Driver", "Data Quality"),
    ("R2550", "Telephonist", "Data Quality"),
    ("R2560", "Gardener/Groundsperson", "Data Quality"),
    ("R2640", "Bricklayer", "Data Quality"),
    ("R2670", "Chargehand", "Data Quality"),
    ("R2680", "Supervisor", "Data Quality"),
    ("R2690", "Engineer", "Data Quality"),
    ("R2700", "Building Officer", "Data Quality"),
    ("R5000", "Network Administrator", "Data Quality"),
    ("R5003", "Cluster System Administrator", "Data Quality"),
    ("R5007", "System Administrator", "Data Quality"),
    ("R5010", "Network Technician", "Data Quality"),
    ("R5020", "Helpdesk Administrator", "Data Quality"),
    ("R5030", "Helpdesk Technician", "Data Quality"),
    ("R5040", "Desktop Support Administrator", "Data Quality"),
    ("R5050", "Desktop Support Technician", "Data Quality"),
    ("R5060", "Security Policy Controller", "Data Quality"),
    ("R5070", "Senior Security Manager", "Data Quality"),
    ("R5072", "Root Registration Authority Manager", "Data Quality"),
    ("R5077", "OBSOLETE SHA Registration Authority", "Data Quality"),
    ("R5080", "Registration Authority Manager", "Data Quality"),
    ("R5090", "Registration Authority Agent", "Data Quality"),
    ("R5100", "Audit Manager", "Data Quality"),
    ("R5105", "Caldicott Guardian", "Data Quality"),
    ("R5110", "Demographic Administrator", "Data Quality"),
    ("R5120", "ISP Administrator", "Data Quality"),
    ("R5130", "Technical Codes Administrator", "Data Quality"),
    ("R5140", "OSS Administrator", "Data Quality"),
    ("R5150", "System Worker", "Data Quality"),
    ("R5170", "End Point Administrator", "Data Quality"),
    ("R5175", "End Point Viewer", "Data Quality"),
    ("R5180", "NASP Service Manager", "Data Quality"),
    ("R5181", "RTS Dashboard User", "Data Quality"),
    ("R5182", "ERS ETP System Administrator", "Data Quality"),
    ("R5183", "RTS BT Dashboard User", "Data Quality"),
    ("R5184", "ERS Spine SLA Manager", "Data Quality"),
    ("R5185", "ERS BT Customer SLA Manager", "Data Quality"),
    ("R5186", "ERS BT Customer SLA User", "Data Quality"),
    ("R5187", "ERS BT Supplier SLA Manager", "Data Quality"),
    ("R5188", "ERS BT Supplier SLA User", "Data Quality"),
    ("R5189", "ERS LogicaCMG SLA User", "Data Quality"),
    ("R5190", "Content Creator", "Data Quality"),
    ("R5191", "ERS Support Administrator", "Data Quality"),
    ("R5192", "ECS Administrator", "Data Quality"),
    ("R5195", "Content Publisher", "Data Quality"),
    ("R5200", "OBSOLETE Service Registration Authority Agent", "Data Quality"),
    ("R5210", "User Details Administrator", "Data Quality"),
    ("R5250", "EBS Administrator", "Data Quality"),
    ("R5300", "Portal Administrator", "Data Quality"),
    ("R5310", "LiquidLogic Administrator", "Data Quality"),
    ("R5320", "i.EPR Administrator", "Data Quality"),
    ("R5330", "Synergy Administrator", "Data Quality"),
    ("R5340", "SystmOne Administrator", "Data Quality"),
    ("R5400", "Availability Monitor", "Data Quality"),
    ("R6010", "Appointments Clerk", "Data Quality"),
    ("R6020", "Outpatient Manager", "Data Quality"),
    ("R6030", "Ward Clerk", "Data Quality"),
    ("R6040", "Bed Manager", "Data Quality"),
    ("R6050", "Clinical Coder", "Data Quality"),
    ("R6060", "Medical Records Clerk", "Data Quality"),
    ("R6070", "Medical Records Manager", "Data Quality"),
    ("R6080", "Waiting List Clerk", "Data Quality"),
    ("R6090", "Waiting List Manager", "Data Quality"),
    ("R6100", "Mental Health Act Administrator", "Data Quality"),
    ("R6130", "Ad-hoc Reporting Server Administrator", "Data Quality"),
    ("R6160", "Ad-hoc Report Manager", "Data Quality"),
    ("R7000", "Outpatient Clerk", "Data Quality"),
    ("R7010", "Outpatient Manager (R7010)", "Data Quality"),
    ("R7020", "Ward Clerk (R7020)", "Data Quality"),
    ("R7050", "Medical Records Clerk (R7050)", "Data Quality"),
    ("R7070", "Waiting List Clerk (R7070)", "Data Quality"),
    ("R7080", "Waiting List Manager (R7080)", "Data Quality"),
    ("R7090", "Mental Health Act Administrator (R7090)", "Data Quality"),
    ("R7100", "Trainer", "Data Quality"),
    ("R7110", "Training Manager", "Data Quality"),
    ("R7120", "Directory of Services Coordinator", "Data Quality"),
    ("R7130", "PAS Manager", "Data Quality"),
    ("R8008", "Admin/Clinical Support Access Role", "Data Quality"),
    ("R8009", "Receptionist Access Role", "Data Quality"),
    ("R8010", "Clerical Access Role", "Data Quality"),
    ("R8011", "Clerical Manager Access Role", "Data Quality"),
    ("R8012", "Information Officer Access Role", "Data Quality"),
    ("R8013", "Health Records Manager Access Role", "Data Quality"),
    ("R8015", "Systems Support Access Role", "Data Quality"),
    ("R9002", "OBSOLETE EBS ClinicalAdmin (Referrer)", "Data Quality"),
    ("R9003", "OBSOLETE EBS Admin (Referrer)", "Data Quality"),
    ("R9004", "OBSOLETE EBS Admin (Trust)", "Data Quality"),
    ("R9007", "OBSOLETE EBS Admin (BMS)", "Data Quality"),
    ("R9008", "OBSOLETE EBS Admin (Helpdesk)", "Data Quality"),
    ("R9009", "OBSOLETE EBS Commissioner", "Data Quality"),
    ("R9010", "OBSOLETE EBS GuidanceDefiner", "Data Quality"),
    ("R9011", "OBSOLETE EBS ServiceDefiner", "Data Quality"),
    ("R9012", "OBSOLETE EBS Information Analyst", "Data Quality"),
    ("R9756", "ETP System Administrator", "Data Quality"),
    ("R9801", "Care Co-ordinators", "Data Quality"),
    # --- Unknown ---
    ("E1005", "Unknown", "Unknown"),
    ("Not Recorded", "Unknown", "Unknown"),
]


# Codes whose SDS description unambiguously identifies them as an ARRS
# scheme role. Kept narrow on purpose — generic codes (R1290 Pharmacist,
# R1110 Physiotherapist, R1070 Paramedic) are NOT flagged because non-ARRS
# staff hold them too (hospital physios, community pharmacists etc.).
#
# The R98xx ACP codes for professions that are only partially ARRS-eligible
# (R9802 Podiatrist ACP, R9805 Dietitian ACP, R9810 OT ACP, R9814 Physio ACP)
# are deliberately FALSE — Advanced Clinical Practitioner ≠ First Contact
# Practitioner, and non-ARRS ACPs exist. R9806 "First Contact Physiotherapists"
# is explicitly FCP so is TRUE.
ARRS_CODES = {
    "R9801",  # Care Co-ordinators
    "R9803",  # Clinical Pharmacist Advanced Practitioner
    "R9804",  # Clinical Pharmacists
    "R9806",  # First Contact Physiotherapists
    "R9807",  # Health and Wellbeing Coaches
    "R9808",  # Mental Health Practitioners
    "R9809",  # Nursing Associates (ARRS-eligible from 2022/23 DES)
    "R9811",  # Paramedic Advanced Practitioner (ARRS paramedic)
    "R9812",  # Pharmacy Technicians
    "R9813",  # Physician Associates
    "R9815",  # Social Prescribing Link Workers
    "R9816",  # Trainee Nursing Associates
}


# Our analytical mapping. Deterministic lookup by role_code — no fuzzy matching.
# Rationale is captured in comments where the call isn't obvious.
ANALYTICAL: dict[str, str] = {
    # GP — all SDS GP codes
    "R0110": "GP", "R0120": "GP", "R0130": "GP", "R0190": "GP",
    "R0215": "GP", "R0220": "GP", "R0230": "GP", "R0260": "GP",
    "R0261": "GP", "R0262": "GP", "R0263": "GP", "R0270": "GP",
    "R1547": "GP",  # corrected — was Physician Associate in old mapping
    "R6200": "GP", "R6300": "GP", "R8000": "GP", "R9001": "GP",
    # Nurse — all SDS Nurses except nursing associates (broken out)
    "E1002": "Nurse", "R0006": "Nurse", "R0330": "Nurse", "R0340": "Nurse",
    "R0350": "Nurse", "R0360": "Nurse", "R0390": "Nurse", "R0400": "Nurse",
    "R0410": "Nurse", "R0420": "Nurse", "R0430": "Nurse", "R0440": "Nurse",
    "R0450": "Nurse", "R0560": "Nurse", "R0570": "Nurse", "R0580": "Nurse",
    "R0590": "Nurse", "R0600": "Nurse", "R0610": "Nurse", "R0620": "Nurse",
    "R0630": "Nurse", "R0690": "Nurse", "R0700": "Nurse", "R1490": "Nurse",
    "R1543": "Nurse", "R1640": "Nurse", "R1974": "Nurse", "R1975": "Nurse",
    "R1976": "Nurse", "R8001": "Nurse", "R8002": "Nurse",
    "R9100": "Nurse",  # A&E Staff Nurse
    # Nursing Associate — ARRS-eligible, distinct from nurse
    "R9809": "Nursing Associate", "R9816": "Nursing Associate",
    # Other Doctor (consultant / specialist / trainee doctors, not GPs)
    "R0010": "Other Doctor",  # Medical Director
    "R0014": "Other Doctor",  # Radiologist
    "R0020": "Other Doctor", "R0050": "Other Doctor", "R0060": "Other Doctor",
    "R0070": "Other Doctor", "R0080": "Other Doctor", "R0090": "Other Doctor",
    "R0100": "Other Doctor",  # Clinical Assistant (historically junior doctor)
    "R0140": "Other Doctor", "R0150": "Other Doctor", "R0160": "Other Doctor",
    "R0170": "Other Doctor", "R0180": "Other Doctor", "R0200": "Other Doctor",
    "R0210": "Other Doctor", "R1981": "Other Doctor",  # Psychiatrist
    "R6400": "Other Doctor",  # Medical Student
    "R9102": "Other Doctor",  # A&E Doctor
    # Pharmacist
    "R1290": "Pharmacist", "R1660": "Pharmacist",
    "R5240": "Pharmacist", "R5260": "Pharmacist",
    "R9803": "Pharmacist", "R9804": "Pharmacist",
    # Pharmacy Technician
    "R1979": "Pharmacy Technician",  # Medical Technical Officer - Pharmacy
    "R5220": "Pharmacy Technician", "R5230": "Pharmacy Technician",
    "R9812": "Pharmacy Technician",
    # HCA / support
    "E1008": "HCA",  # Healthcare Science Assistant
    "R0017": "HCA",  # Assistant Practitioner (generic)
    "R1450": "HCA", "R1460": "HCA", "R1470": "HCA", "R1480": "HCA",
    "R1540": "HCA",  # Associate Practitioner (band 4 support worker)
    "R1560": "HCA", "R1590": "HCA", "R1650": "HCA",
    "R2500": "HCA", "R2600": "HCA", "R2740": "HCA",
    # Physician Associate
    "E1003": "Physician Associate",  # Physician Assistant (old name)
    "R9813": "Physician Associate",
    # Paramedic
    "R1070": "Paramedic", "R1080": "Paramedic",
    "R1090": "Paramedic", "R1100": "Paramedic",
    "R9811": "Paramedic",
    # Physiotherapist
    "R0500": "Physiotherapist",  # student
    "R1110": "Physiotherapist", "R1120": "Physiotherapist",
    "R1130": "Physiotherapist", "R1140": "Physiotherapist",
    "R9806": "Physiotherapist", "R9814": "Physiotherapist",
    # Counsellor / psychological therapies
    "R0550": "Counsellor",  # Student Psychotherapist
    "R1300": "Counsellor",  # Psychotherapist
    "R1310": "Counsellor",  # Clinical Psychologist
    "R1550": "Counsellor",
    "R1670": "Counsellor", "R1680": "Counsellor",
    # Care Navigator / Care Coordinator
    "R9801": "Care Navigator",
    # Mental Health Practitioner (ARRS)
    "R9808": "Mental Health Practitioner",
    # Health & Wellbeing Coach (ARRS)
    "R9807": "Health & Wellbeing Coach",
    # Social Prescriber (ARRS)
    "R9815": "Social Prescriber",
    # Admin — SDS Data Quality group bulk-mapped; plus handful of ODPC admin codes
    "R0002": "Admin",  # Porter
    "R0003": "Admin",  # Clinical Application Administrator
    "R0015": "Admin",  # PACS Administrator
    "R0247": "Admin",  # Other Community Health Service - Admin Clerk
    "R8006": "Admin",  # Medical Secretary Access Role
    "R8007": "Admin",  # Clinical Coder Access Role
    "R9101": "Admin",  # A&E Manager
    "R9104": "Admin",  # A&E Clerk
    # Non-clinical catch-all (social care, chaplain, home help)
    "R0243": "Other",  # Other Community Health Service - Social Care Worker
    "R1320": "Other",  # Chaplain
    "R1330": "Other",  # Social Worker
    "R1340": "Other",  # Approved Social Worker
    "R1350": "Other",  # Youth Worker
    "R1980": "Other",  # Patient Welfare Officer
    "R8014": "Other",  # Social Worker Access Role
    "R8024": "Other",  # Bank Access Role
    # Social services roles (R9500-R9725) — SDS bucket is ODPC but these
    # are local-authority social care, not clinical. Reclassify to Other.
    # Exceptions: R9550/R9555/R9560 (OT — clinical) stay Other Clinical;
    # R9730-R9745 Intermediate Care stay Other Clinical (health-adjacent);
    # R9750/R9755 Social Care SAP → Admin (system roles).
    "R9500": "Other", "R9505": "Other", "R9510": "Other",
    "R9515": "Other", "R9520": "Other", "R9525": "Other",
    "R9530": "Other", "R9535": "Other", "R9540": "Other", "R9545": "Other",
    "R9565": "Other", "R9570": "Other", "R9575": "Other", "R9580": "Other",
    "R9585": "Other", "R9590": "Other", "R9595": "Other", "R9600": "Other",
    "R9605": "Other", "R9615": "Other", "R9620": "Other", "R9625": "Other",
    "R9630": "Other", "R9635": "Other", "R9640": "Other", "R9645": "Other",
    "R9650": "Other", "R9655": "Other", "R9660": "Other", "R9665": "Other",
    "R9670": "Other", "R9675": "Other", "R9680": "Other",
    "R9685": "Other", "R9690": "Other", "R9695": "Other",
    "R9700": "Other", "R9705": "Other",
    "R9710": "Other", "R9715": "Other", "R9720": "Other", "R9725": "Other",
    "R9750": "Admin", "R9755": "Admin",
    # Unknown
    "E1005": "Unknown", "Not Recorded": "Unknown",
}


def classify(role_code: str, sds_group: str) -> str:
    """Return the analytical practitioner_role_group for a role code.

    Explicit lookup wins. Anything not in ANALYTICAL falls through to a
    sensible default based on its SDS group (Data Quality → Admin, ODPC →
    Other Clinical, etc.). Every code in RAW should end up in a bucket.
    """
    if role_code in ANALYTICAL:
        return ANALYTICAL[role_code]
    if sds_group == "Data Quality":
        return "Admin"
    if sds_group == "Other Direct Patient Care":
        return "Other Clinical"
    if sds_group == "Unknown":
        return "Unknown"
    # Shouldn't reach here — GP / Nurses are fully enumerated above
    raise ValueError(f"Unclassified: {role_code} ({sds_group})")


def main() -> None:
    rows = ["role_code,role_description,sds_role_group,practitioner_role_group,is_arrs_role"]
    seen: set[str] = set()
    for code, desc, sds_group in RAW:
        if code in seen:
            raise ValueError(f"Duplicate role_code in RAW: {code}")
        seen.add(code)
        analytical = classify(code, sds_group)
        arrs = "true" if code in ARRS_CODES else "false"
        # Quote descriptions containing commas or quotes
        if "," in desc or '"' in desc:
            desc_out = '"' + desc.replace('"', '""') + '"'
        else:
            desc_out = desc
        rows.append(f"{code},{desc_out},{sds_group},{analytical},{arrs}")

    out_path = Path(__file__).resolve().parents[2] / "seeds" / "sds_role_groups.csv"
    out_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    print(f"Wrote {len(rows) - 1} rows to {out_path}")

    # Emit a quick summary so the reviewer can sanity-check bucket sizes
    from collections import Counter
    analytical_counts = Counter(
        classify(code, sds_group) for code, _, sds_group in RAW
    )
    print("\npractitioner_role_group distribution:")
    for bucket, n in sorted(analytical_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {bucket:32s} {n:>4d}")

    arrs_count = sum(1 for code, _, _ in RAW if code in ARRS_CODES)
    print(f"\nis_arrs_role TRUE count: {arrs_count}")


if __name__ == "__main__":
    main()
