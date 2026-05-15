epiq-edp-dl-dev-silver  ----> silver\_sap\_db



epiq-edp-dl-dev-gold ----> gold\_sap\_db







2024-01-01 → 2024-02-29

2024-03-01 → 2024-04-30

2024-05-01 → 2024-06-30

2024-07-01 → 2024-08-31

2024-09-01 → 2024-10-31

2024-11-01 → 2024-12-31









aws glue start-job-run \\

&nbsp; --job-name edp-sap-bronze-contracts-items-copy \\

&nbsp; --arguments '{

&nbsp;   "--bucket\_name":"epiq-edp-dl-dev-bronze",

&nbsp;   "--start\_date":"2024-03-01",

&nbsp;   "--end\_date":"2024-04-30",

&nbsp;   "--entity":"ContractItems",

&nbsp;   "--secret\_name":"sap/dev/odata/credentials",

&nbsp;   "--source\_system":"sap"

&nbsp; }'

