package org.embulk.filter.pseudo;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;


import org.embulk.config.Config;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;

public class PseudoFilterPlugin implements FilterPlugin {
    public interface PluginTask extends Task {
        @Config("secret_key")
        public String getSecretKey();

        @Config("column_names")
        public List<String> getColumnNames();
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema, FilterPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema outputSchema = inputSchema;

        control.run(task.dump(), outputSchema);
    }

    private Mac getMac(PluginTask task) throws NoSuchAlgorithmException, InvalidKeyException,
            UnsupportedEncodingException {
        Mac mac = Mac.getInstance("HmacSHA256");
        SecretKeySpec secretKeySpec = new SecretKeySpec(task.getSecretKey().getBytes("UTF-8"), "HmacSHA256");
        mac.init(secretKeySpec);
        return mac;
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema, Schema outputSchema, PageOutput output) {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final Mac hmac;

        try {
            hmac = getMac(task);
        } catch (Exception ex) {
            throw new ConfigException(ex);
        }

        final int[] targetColumns = new int[task.getColumnNames().size()];
        int i = 0;
        for (String name : task.getColumnNames()) {
            targetColumns[i++] = inputSchema.lookupColumn(name).getIndex();
        }

        return new PageOutput() {
            private PageReader pageReader = new PageReader(inputSchema);
            private PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            @Override
            public void add(Page page) {
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    // columnvisitor is only ann interface...
                    inputSchema.visitColumns(new ColumnVisitor() {
                        @Override
                        public void booleanColumn(Column column) {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            } else {
                                pageBuilder.setBoolean(column, pageReader.getBoolean(column));
                            }
                        }

                        @Override
                        public void longColumn(Column column) {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            } else {
                                pageBuilder.setLong(column, pageReader.getLong(column));
                            }
                        }

                        @Override
                        public void doubleColumn(Column column) {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            } else {
                                pageBuilder.setDouble(column, pageReader.getDouble(column));
                            }
                        }

                        @Override
                        public void stringColumn(Column column) {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            } else if (isTargetColumn(column)) {
                                String orig = pageReader.getString(column);
                                byte[] result;
                                try {
                                    result = hmac.doFinal(orig.getBytes("UTF-8"));
                                } catch (IllegalStateException | UnsupportedEncodingException ex) {
                                    throw new DataException(ex);
                                }
                                String encoded = Base64.getEncoder().encodeToString(result);
                                pageBuilder.setString(column, encoded);
                            }
                            else {
                                pageBuilder.setString(column, pageReader.getString(column));
                            }
                        }

                        @Override
                        public void timestampColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            }
                            else {
                                pageBuilder.setTimestamp(column, pageReader.getTimestamp(column));
                            }
                        }

                        @Override
                        public void jsonColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                pageBuilder.setNull(column);
                            }
                            else {
                                pageBuilder.setJson(column, pageReader.getJson(column));
                            }
                        }
                    });
                    pageBuilder.addRecord();
                }
            }

			@Override
			public void finish() {
			    pageBuilder.finish();				
			}

			@Override
			public void close() {
                pageBuilder.close();				
			}
            
            private boolean isTargetColumn(Column c)
            {
                for (int i = 0; i < targetColumns.length; i++) {
                    if (c.getIndex() == targetColumns[i]) {
                        return true;
                    }
                }
                return false;
            }

        };
    }
}
