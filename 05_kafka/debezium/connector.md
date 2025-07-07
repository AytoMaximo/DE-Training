Как только все развернется, то нам необходимо настроить коннектор для определенной таблицы,
которую будет отслеживать дебезиум. Это делается в любом терминале при помощи следующей команды -

(адаптировано под PowerShell)

```powershell
curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d "@W:\Courses\Stepik-DE-Training\DE-Training\05_kafka\debezium\connector.json"
```
