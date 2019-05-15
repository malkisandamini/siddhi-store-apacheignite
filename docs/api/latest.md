# API Docs - v1.0.0-SNAPSHOT

## Store

### apacheignite *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#store">(Store)</a>*

<p style="word-wrap: break-word">This extension connects to apache Ignite store.It also implements read-write operations on connected apache ignite data store.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@Store(type="apacheignite", url="<STRING>", auth.enabled="<STRING>", username="<STRING>", password="<STRING>", table.name="<STRING>", schema="<STRING>", template="<STRING>", distribute.joins="<STRING>", enforce.join.order="<STRING>", collocated="<STRING>", replicated.only="<STRING>", auto.close.server.cursor="<STRING>", socket.send.buffer="<STRING>", socket.receive.buffer="<STRING>", backups="<STRING>", atomicity="<STRING>", affinity.key="<STRING>", cache.name="<STRING>", data.region="<STRING>")
@PrimaryKey("PRIMARY_KEY")
@Index("INDEX")
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">url</td>
        <td style="vertical-align: top; word-wrap: break-word">Describes the url required for establishing the connection with apache ignitestore. </td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">auth.enabled</td>
        <td style="vertical-align: top; word-wrap: break-word">Describes whether authentication is enabled or not </td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">username</td>
        <td style="vertical-align: top; word-wrap: break-word">username for SQL connection.Mandatory parameter if the authentication is enabled on the server </td>
        <td style="vertical-align: top">ignite </td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">password</td>
        <td style="vertical-align: top; word-wrap: break-word">password for SQL connection.Mandatory parameter if the authentication is enabled on the server. </td>
        <td style="vertical-align: top">ignite</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">table.name</td>
        <td style="vertical-align: top; word-wrap: break-word">The name with which the Siddhi store must be persisted in the Apache Ignite store.</td>
        <td style="vertical-align: top">The table name defined in the Siddhi Application query.</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">schema</td>
        <td style="vertical-align: top; word-wrap: break-word">Schema name to access.Possible values for defining schema are public,ignite and any custom schema defined by user. </td>
        <td style="vertical-align: top">Public</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">template</td>
        <td style="vertical-align: top; word-wrap: break-word"> name of a cache template registered in Ignite to use as a configuration for the distributed cache.The possible values are partitioned and replicated. </td>
        <td style="vertical-align: top">partitioned </td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">distribute.joins</td>
        <td style="vertical-align: top; word-wrap: break-word">Whether to use distributed joins for non collocated data or not. </td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">enforce.join.order</td>
        <td style="vertical-align: top; word-wrap: break-word">Whether to enforce join order of tables in the query or not. If set to true query optimizer will not reorder tables in join. </td>
        <td style="vertical-align: top">false </td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">collocated</td>
        <td style="vertical-align: top; word-wrap: break-word">Whether your data is co-located or not </td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">replicated.only</td>
        <td style="vertical-align: top; word-wrap: break-word">Whether query contains only replicated tables or not </td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">auto.close.server.cursor</td>
        <td style="vertical-align: top; word-wrap: break-word">Whether to close server-side cursor automatically when last piece of result set is retrieved or not. </td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">socket.send.buffer</td>
        <td style="vertical-align: top; word-wrap: break-word">Socket send buffer size.When set to 0, OS default will be used. </td>
        <td style="vertical-align: top">0 </td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">socket.receive.buffer</td>
        <td style="vertical-align: top; word-wrap: break-word">Socket receive buffer size.When set to 0, OS default will be used. </td>
        <td style="vertical-align: top">0</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">backups</td>
        <td style="vertical-align: top; word-wrap: break-word">Number of backup copies of data.It can take the value of any positive integer</td>
        <td style="vertical-align: top">0</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">atomicity</td>
        <td style="vertical-align: top; word-wrap: break-word">Sets atomicity mode for the cache.The possible values for atomicity are atomic,transactional and transactional_snapshot. </td>
        <td style="vertical-align: top">atomic </td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">affinity.key</td>
        <td style="vertical-align: top; word-wrap: break-word">specifies an affinity key name which is a column of the primary key constraint.</td>
        <td style="vertical-align: top"> column of the primary key constraint. </td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">cache.name</td>
        <td style="vertical-align: top; word-wrap: break-word">Name of the cache created.It can take any custom name defined by user and default cache name takes the format {schema}_SQL_{table.name}</td>
        <td style="vertical-align: top"> default name of the new cache. </td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">data.region</td>
        <td style="vertical-align: top; word-wrap: break-word">Name of the data region where table entries should be stored. </td>
        <td style="vertical-align: top">an existing data region name </td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream StockStream (symbol string, price float, volume long);
 @Store(type="apacheignite", url = " jdbc:ignite:thin://127.0.0.1 " ,,auth.enabled = "true",username="ignite ", password=" ignite ) 
@PrimaryKey("symbol")
define table StockTable (symbol string, price float, volume long);
@info(name = 'query1') 
from StockStream
insert into StockTable ; 
```
<p style="word-wrap: break-word">The above example creates a table in apache ignite data store if it does not exists already with 'symbol' as the primary key.The connection is made as specifiedby the parameters configured under '@Store' annotation.Data is inserted into table,stockTable from stockStream</p>

<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
define stream StockStream (symbol string, price float, volume long);
 @Store(type="apacheignite", url = " jdbc:ignite:thin://127.0.0.1 " ,username="ignite ", password=" ignite ) 
@PrimaryKey("symbol")
define table StockTable (symbol string, price float, volume long);
@info(name = 'query2')
 from FooStream#window.length(1) join StockTable on StockTable.symbol==FooStream.name 
select StockTable.symbol as checkName, StockTable.volume as checkVolume,StockTable.price as checkCategory
 insert into OutputStream;
```
<p style="word-wrap: break-word">The above example creates a table in apache ignite data store if it does not exists already with 'symbol' as the primary key.The connection is made as specifiedby the parameters configured under '@Store' annotation.Then the table is joined with a stream name 'FooStream' based on a condition. The following operations are included in the condition:<br>[AND, OR, Comparisons(&lt;, &lt;=, &gt;, &gt;=, ==, != )]</p>

