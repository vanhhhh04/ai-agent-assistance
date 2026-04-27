ec3159a9a1339ef0eb0fde86852280fadc2c914fdb91fcaa

docker compose exec data-source python sim_warehouse.py
docker compose exec data-source python sim_erp.py
docker compose exec data-source python sim_payment.py
rm -rf data/kafka/* data/zookeeper/data/* data/zookeeper/log/*
rm -rf ./nifi/flowfile_repository ./nifi/content_repository ./nifi/provenance_repository
docker exec -u root nifi chmod 777 /opt/nifi/csv_input
docker compose rm -f nifi 


  What happens on each bash cli/startup.sh run                                                        
                                                                                                      
  1. Kafka + ZooKeeper state wiped → clean slate, no corruption carried over
  2. docker compose up -d → fresh Kafka + ZK come up; Postgres/NiFi/HDFS/Hive/Airflow keep their state
   (unaffected)                                                                                       
  3. Debezium connector registers → triggers fresh snapshot from Postgres → re-populates erp.public.* 
  Kafka topics with all 187k customers, 1M orders, etc. (takes ~30-60 sec)                            
  4. NiFi flow created if missing (if still there, skipped) 
  5. CSV perms fixed                                                                                  
  6. Sims started                                           
  7. Status printed 

   bash cli/sim-logs.sh erp        # nên thấy INSERT order/UPDATE status mỗi vài giây
  bash cli/sim-logs.sh warehouse  # stock_update mỗi 10s, product mới mỗi 2 phút
  bash cli/sim-logs.sh payment    # INSERT payment mỗi 3s, shipping mỗi 15s


  Ngày thường: startup.sh → pipeline-status.sh → làm việc.
Debug data: sim-logs.sh + sample-data.sh + verify-pipeline.sh.
NiFi trục trặc: thử nifi-recover.sh trước, chưa ổn mới nifi-reset.sh.
Làm demo sạch hoàn toàn: wipe.sh rồi startup.sh.