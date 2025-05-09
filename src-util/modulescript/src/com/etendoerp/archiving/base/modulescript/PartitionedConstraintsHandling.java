package com.etendoerp.archiving.modulescript;

import java.sql.PreparedStatement;

import org.openbravo.database.ConnectionProvider;
import org.openbravo.modulescript.ModuleScript;

public class PartitionedConstraintsHandling extends ModuleScript {

  public void execute() {
    try {
      ConnectionProvider cp = getConnectionProvider();

      // DROP
      StringBuilder sql = new StringBuilder("SELECT 1 FROM pg_partitioned_table WHERE partrelid = 'fact_acct'::regclass;");
      PreparedStatement ps = cp.getPreparedStatement(
          sql.toString()
      );
      // run the query and get if it is partitioned based on the result
      boolean isPartitioned = ps.executeQuery().next();
      ps.close();

      sql = new StringBuilder();
      sql.append("UPDATE FACT_ACCT_CFS SET EM_ETABASE_DATEACCT = F.DATEACCT FROM FACT_ACCT F WHERE F.FACT_ACCT_ID = FACT_ACCT_CFS.FACT_ACCT_ID AND EM_ETABASE_DATEACCT IS NULL;");
      sql.append("UPDATE FACT_ACCT_CFS SET EM_ETABASE_DATEACCT_REF = F.DATEACCT FROM FACT_ACCT F WHERE F.FACT_ACCT_ID = FACT_ACCT_CFS.FACT_ACCT_REF_ID AND EM_ETABASE_DATEACCT_REF IS NULL;");
      // REMOVE FK
      sql.append("ALTER TABLE IF EXISTS PUBLIC.FACT_ACCT_CFS\n");
      sql.append("DROP CONSTRAINT IF EXISTS FACT_ACCT_CFS_FACT_ACCT;\n");
      sql.append("ALTER TABLE IF EXISTS PUBLIC.FACT_ACCT_CFS\n");
      sql.append("DROP CONSTRAINT IF EXISTS FACT_ACCT_CFS_FACT_ACCT1;\n");
      // ADD FK
      if(isPartitioned) {
        sql.append("ALTER TABLE IF EXISTS PUBLIC.FACT_ACCT_CFS\n");
        sql.append(
            "ADD CONSTRAINT FACT_ACCT_CFS_FACT_ACCT FOREIGN KEY (FACT_ACCT_REF_ID, EM_ETABASE_DATEACCT_REF) REFERENCES PUBLIC.FACT_ACCT (FACT_ACCT_ID, DATEACCT) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION;\n");
        sql.append("ALTER TABLE IF EXISTS PUBLIC.FACT_ACCT_CFS\n");
        sql.append(
            "ADD CONSTRAINT FACT_ACCT_CFS_FACT_ACCT1 FOREIGN KEY (FACT_ACCT_ID, EM_ETABASE_DATEACCT) REFERENCES PUBLIC.FACT_ACCT (FACT_ACCT_ID, DATEACCT) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE;\n");
      } else {
        sql.append("ALTER TABLE IF EXISTS PUBLIC.FACT_ACCT_CFS\n");
        sql.append(
            "ADD CONSTRAINT FACT_ACCT_CFS_FACT_ACCT FOREIGN KEY (FACT_ACCT_REF_ID) REFERENCES PUBLIC.FACT_ACCT (FACT_ACCT_ID) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION;\n");
        sql.append("ALTER TABLE IF EXISTS PUBLIC.FACT_ACCT_CFS\n");
        sql.append(
            "ADD CONSTRAINT FACT_ACCT_CFS_FACT_ACCT1 FOREIGN KEY (FACT_ACCT_ID) REFERENCES PUBLIC.FACT_ACCT (FACT_ACCT_ID) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE CASCADE;\n");
      }
      ps = cp.getPreparedStatement(
          sql.toString()
      );
      ps.executeUpdate();
    } catch (Exception e) {
      handleError(e);
    }
  }
}
