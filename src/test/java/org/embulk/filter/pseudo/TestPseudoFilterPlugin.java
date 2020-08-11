package org.embulk.filter.pseudo;

import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.*;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.type.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;

import static org.embulk.spi.PageTestUtils.buildPage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

public class TestPseudoFilterPlugin {

  @Rule
  public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private PseudoFilterPlugin plugin;

  private ConfigSource defaultConfig()
  {
      return runtime.getExec().newConfigSource()
              .set("type", "pseudo")
              .set("secret_key", "secret123");
  }

  @Before
  public void setup()
  {
      plugin = new PseudoFilterPlugin();
  }

  @Test
  public void itWorks() {
    ConfigSource config = defaultConfig()
      .set("column_names", ImmutableList.of("to_be_macced_column"));


    Schema schema = Schema.builder()
      .add("to_be_macced_column", Types.STRING)
      .build();

      List rawRecord = ImmutableList.of("My super secret");
      List filteredRecord = applyFilter(config, schema, rawRecord);

      String hmactext = (String) filteredRecord.get(0);

      assertEquals(hmactext, "TAqk5ZComW1fsDLU5kifn2U+lWbADRkpHfryE+wvrYc=");
      
  }

  @Test
  public void itProducesSameResultsForSameinput() {
    ConfigSource config = defaultConfig()
      .set("column_names", ImmutableList.of("to_be_macced_column"));


    Schema schema = Schema.builder()
      .add("to_be_macced_column", Types.STRING)
      .build();

      String firstHmactext  = (String) applyFilter(config, schema, ImmutableList.of("My super secret")).get(0);
      String secondHmactext  = (String) applyFilter(config, schema, ImmutableList.of("My super secret")).get(0);

      assertEquals(firstHmactext, secondHmactext);      
  }


    /** Apply the filter to a single record */
    private PageReader applyFilter(ConfigSource config, final Schema schema, final Object... rawRecord)
    {
        if (rawRecord.length > schema.getColumnCount()) {
            throw new UnsupportedOperationException("applyFilter() only supports a single record, " +
                    "number of supplied values exceed the schema column size.");
        }

        final MockPageOutput filteredOutput = new MockPageOutput();

        plugin.transaction(config, schema, new FilterPlugin.Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                PageOutput originalOutput = plugin.open(taskSource, schema, outputSchema, filteredOutput);
                originalOutput.add(buildPage(runtime.getBufferAllocator(), schema, rawRecord).get(0));
                originalOutput.finish();
                originalOutput.close();
            }
        });
        assert filteredOutput.pages.size() == 1;

        PageReader reader = new PageReader(schema);
        reader.setPage(filteredOutput.pages.get(0));
        reader.nextRecord();

        return reader;
    }

    /** Conveniently returning a List after apply a filter over the original list */
    private List applyFilter(ConfigSource config, Schema schema, List rawRecord)
    {
        try (PageReader reader = applyFilter(config, schema, rawRecord.toArray())) {
            return readToList(reader, schema);
        }
    }

    private static List readToList(final PageReader reader, Schema schema)
    {
        final Object[] filtered = new Object[schema.getColumnCount()];
        schema.visitColumns(new ColumnVisitor()
        {
            @Override
            public void booleanColumn(Column column)
            {
                filtered[column.getIndex()] = reader.getBoolean(column);
            }

            @Override
            public void longColumn(Column column)
            {
                filtered[column.getIndex()] = reader.getLong(column);
            }

            @Override
            public void doubleColumn(Column column)
            {
                filtered[column.getIndex()] = reader.getDouble(column);
            }

            @Override
            public void stringColumn(Column column)
            {
                filtered[column.getIndex()] = reader.getString(column);
            }

            @Override
            public void timestampColumn(Column column)
            {
                filtered[column.getIndex()] = reader.getTimestamp(column);
            }

            @Override
            public void jsonColumn(Column column)
            {
                filtered[column.getIndex()] = reader.getJson(column);
            }
        });
        return Arrays.asList(filtered);
    }

}
