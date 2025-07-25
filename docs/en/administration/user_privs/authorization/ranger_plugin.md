---
displayed_sidebar: docs
sidebar_position: 40
---

# Manage permissions with Apache Ranger

[Apache Ranger](https://ranger.apache.org/) provides a centralized security management framework that allows users to customize access policies through a visual web page. This helps determine which roles can access which data and exercise fine-grained data access control for various components and services in the Hadoop ecosystem.

Apache Ranger provides the following core modules:

- **Ranger Admin**: the core module of Ranger with a built-in web page. Users can create and update security policies on this page or through a REST interface. Plugins of various components of the Hadoop ecosystem poll and pull these policies at a regular basis.
- **Agent Plugin**: plugins of components embedded in the Hadoop ecosystem. These plugins pull security policies from Ranger Admin on a regular basis and store the policies in local files. When users access a component, the corresponding plugin assesses the request based on the configured security policy and sends the authentication results to the corresponding component.
- **User Sync**: used to pull user and user group information, and synchronize the permission data of users and user groups to Ranger's database.

In addition to the native RBAC privilege system, StarRocks v3.1.9 also supports access control through Apache Ranger. Currently, StarRocks supports:

- Creates access policies, masking policies, and row-level filter policies through Apache Ranger.
- Ranger audit logs.
- **Ranger Servers that use Kerberos for authentication are not supported.**
- You can register multiple StarRocks Services in the same Apache Ranger service to manage privileges in different StarRocks clusters.

This topic describes the permission control methods and integration process of StarRocks and Apache Ranger. For information on how to create security policies on Ranger to manage data security, see the [Apache Ranger official website](https://ranger.apache.org/).

From v3.5.0 onwards, StarRocks supports Group Provider to collect group information from external authentication systems for user group management. For more information, see [Authenticate User Groups](../group_provider.md).

## Permission control method

StarRocks integrated with Apache Ranger provides the following permission control methods:

- Create StarRocks Service in Ranger to implement permission control. When users access StarRocks internal tables, external tables, or other objects, access control is performed according to the access policies configured in StarRocks Service.
- When users access an external data source, the external service (such as the Hive Service) on Apache Ranger can be reused for access control. StarRocks can match Ranger services with different External Catalogs and implements access control based on the Ranger service corresponding to the data source.

After StarRocks is integrating with Apache Ranger, you can achieve the following access control patterns:

- Use Apache Ranger to uniformly manage access to StarRocks internal tables, external tables, and all objects.
- Use Apache Ranger to manage access to StarRocks internal tables and objects. For External Catalogs, reuse the policy of the corresponding external service on Ranger for access control.
- Use Apache Ranger to manage access to External Catalogs by reusing the Service corresponding to the external data source. Use StarRocks native RBAC privilege system to manage access to StarRocks internal tables and objects.

**Authentication process**

1. You can also use LDAP for user authentication, then use Ranger to synchronize LDAP users and configure access rules for them. StarRocks can also complete user login authentication through LDAP.
2. When users initiate a query, StarRocks parses the query statement, passes user information and required privileges to Apache Ranger. Ranger determines whether the user has the required privilege based on the access policy configured in the corresponding Service, and returns the authentication result to StarRocks. If the user has access, StarRocks returns the query data; if not, StarRocks returns an error.

## Prerequisites

- Apache Ranger 2.1.0 or later has been installed. For the instructions on how to install Apache Ranger, see [Ranger quick start](https://ranger.apache.org/quick_start_guide.html).
- All StarRocks FE machines have access to Apache Ranger. You can check this by running the following command on each FE machine:

   ```SQL
   telnet <ranger-ip> <ranger-port>
   ```

   If `Connected to <ip>` is displayed, the connection is successful.

## Integrate StarRocks Service with Ranger

### (Optional) Install ranger-starrocks-plugin

:::note
The main purpose of this step is to use Ranger's resource name autocomplete feature. When authoring policies in Ranger Admin, users need to enter the name of the resources whose access need to be protected. To make it easier for users to enter the resource names, Ranger Admin provides the autocomplete feature, which looks up the available resources in the service that match the input entered so far and automatically completes the resource name.

If you do not have the permissions to operate the Ranger cluster or do not need this feature, you can skip this step.

Also, please notice that if you didn't install the ranger-starrocks-plugin, then you cannot use `test connection` when creating StarRocks service. However, that doesn't mean that you can not create the service successfully.
:::

1. Create the `starrocks` folder in the Ranger Admin directory `ews/webapp/WEB-INF/classes/ranger-plugins`.

   ```SQL
   mkdir {path-to-ranger}/ews/webapp/WEB-INF/classes/ranger-plugins/starrocks
   ```

2. Download [plugin-starrocks/target/ranger-starrocks-plugin-3.0.0-SNAPSHOT.jar](https://www.starrocks.io/download/community) and [mysql-connector-j.jar](https://dev.mysql.com/downloads/connector/j/), and place them in the `starrocks` folder.

3. Restart Ranger Admin.

   ```SQL
   ranger-admin restart
   ```

### Configure StarRocks Service on Ranger Admin

:::note
This step configures the StarRocks Service on Ranger so that users can perform access control on StarRocks objects through Ranger.
:::

1. Copy [ranger-servicedef-starrocks.json](https://github.com/StarRocks/ranger/blob/master/agents-common/src/main/resources/service-defs/ranger-servicedef-starrocks.json) to any directory of the StarRocks FE machine or Ranger machine.

   ```SQL
   wget https://raw.githubusercontent.com/StarRocks/ranger/master/agents-common/src/main/resources/service-defs/ranger-servicedef-starrocks.json
   ```

   :::note
   If you do not need Ranger's autocomplete feature (which means you did not install the ranger-starrocks-plugin), you must set `implClass` in the .json file to empty:

   ```JSON
   "implClass": "",
   ```

   If you need Ranger's autocomplete feature (which means you have installed the ranger-starrocks-plugin), you must set `implClass` in the .json file to `org.apache.ranger.services.starrocks.RangerServiceStarRocks`:

   ```JSON
   "implClass": "org.apache.ranger.services.starrocks.RangerServiceStarRocks",
   ```

   :::

2. Add StarRocks Service by running the following command as a Ranger administrator.

   ```Bash
   curl -u <ranger_adminuser>:<ranger_adminpwd> \
   -X POST -H "Accept: application/json" \
   -H "Content-Type: application/json" http://<ranger-ip>:<ranger-port>/service/plugins/definitions -d@ranger-servicedef-starrocks.json
   ```

3. Access `http://<ranger-ip>:<ranger-host>/login.jsp` to log in to the Apache Ranger page. The STARROCKS service appears on the page.

   ![home](../../../_assets/ranger_home.png)

4. Click the plus sign (`+`) after **STARROCKS** to configure StarRocks Service.

   ![service detail](../../../_assets/ranger_service_details.png)

   ![property](../../../_assets/ranger_properties.png)

   - `Service Name`: You must enter a service name.
   - `Display Name`: The name you want to display for the service under STARROCKS. If it is not specified, `Service Name` will be displayed.
   - `Username` and `Password`: FE username and password, used to auto-complete object names when creating policies. The two parameters do not affect the connectivity between StarRocks and Ranger. If you want to use auto-completion, configure at least one user with the `db_admin` role activated.
   - `jdbc.url`: Enter the StarRocks FE IP address and port.

   The following figure shows a configuration example.

   ![example](../../../_assets/ranger_show_config.png)

   The following figure shows the added service.

   ![added service](../../../_assets/ranger_added_service.png)

5. Click **Test connection** to test the connectivity, and save it after the connection is successful. If you didn't install ranger-starrocks-plugin, then you can skip test connection and create directly.
6. On each FE machine of the StarRocks cluster, create [ranger-starrocks-security.xml](https://github.com/StarRocks/ranger/blob/master/plugin-starrocks/conf/ranger-starrocks-security.xml) in the `fe/conf` folder and copy the content. You must modify the following two parameters and save the modifications:

   - `ranger.plugin.starrocks.service.name`: Change to the name of the StarRocks Service you created in Step 4.
   - `ranger.plugin.starrocks.policy.rest the url`: Change to the address of the Ranger Admin.

   If you need to modify other configurations, refer to official documentation of Apache Ranger. For example, you can modify `ranger.plugin.starrocks.policy.pollIntervalMs` to change the interval for pulling policy changes.

   ```SQL
   vim ranger-starrocks-security.xml

   ...
       <property>
           <name>ranger.plugin.starrocks.service.name</name>
           <value>starrocks</value> -- Change it to the StarRocks Service name.
           <description>
               Name of the Ranger service containing policies for this StarRocks instance
           </description>
       </property>
   ...

   ...
       <property>
           <name>ranger.plugin.starrocks.policy.rest.url</name>
           <value>http://localhost:6080</value> -- Change it to Ranger Admin address.
           <description>
               URL to Ranger Admin
           </description>
       </property>   
   ...
   ```

7. (Optional) If you want to use the Audit Log service of Ranger, you need to create the [ranger-starrocks-audit.xml](https://github.com/StarRocks/ranger/blob/master/plugin-starrocks/conf/ranger-starrocks-audit.xml) file in the `fe/conf` folder of each FE machine. Copy the content, **replace `solr_url` in `xasecure.audit.solr.solr_url` with your own `solr_url`**, and save the file.

8. Add the configuration `access_control = ranger` to all FE configuration files.

   ```SQL
   vim fe.conf
   access_control=ranger 
   ```

9. Restart all FE machines.

   ```SQL
   -- Switch to the FE folder. 
   cd..

   bin/stop_fe.sh
   bin/start_fe.sh
   ```

## Reuse other services to control access to external tables

For External Catalog, you can reuse external services (such as Hive Service) for access control. StarRocks supports matching different Ranger external services for different Catalogs. When users access an external table, the system implements access control based on the access policy of the Ranger Service corresponding to the external table. The user permissions are consistent with the Ranger user with the same name.

1. Copy Hive's Ranger configuration files [ranger-hive-security.xml](https://github.com/StarRocks/ranger/blob/master/hive-agent/conf/ranger-hive-security.xml) and [ranger-hive-audit.xml](https://github.com/StarRocks/ranger/blob/master/hive-agent/conf/ranger-hive-audit.xml) to the `fe/conf` file of all FE machines. Make sure Ranger's IP and port are correct.
2. Restart all FE machines.
3. Configure External Catalog.

   - When you create an External Catalog, add the property `"ranger.plugin.hive.service.name"`.

      ```SQL
        CREATE EXTERNAL CATALOG hive_catalog_1
        PROPERTIES (
            "type" = "hive",
            "hive.metastore.type" = "hive",
            "hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083",
            "ranger.plugin.hive.service.name" = "<ranger_hive_service_name>"
        )
      ```

   - You can also add this property to an existing External Catalog.
  
       ```SQL
       ALTER CATALOG hive_catalog_1
       SET ("ranger.plugin.hive.service.name" = "<ranger_hive_service_name>");
       ```

​    This operation changes the authentication method of an existing Catalog to Ranger-based authentication.

## What to do next

After adding a StarRocks Service, you can click the service to create access control policies for the service and assign different permissions to different users or user groups. When users access StarRocks data, access control will be implemented based on these policies.
