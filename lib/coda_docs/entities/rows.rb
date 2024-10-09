module CodaDocs
  module Entities
    class Rows < Entity
      def all(doc_id, table_id, options = nil)
        response = connection.get("/docs/#{doc_id}/tables/#{table_id}/rows", query: options)
        parse_response(response)
      end

      def find(doc_id, table_id, row_id)
        connection.get("/docs/#{doc_id}/tables/#{table_id}/rows/#{row_id}")
      end

      def delete(doc_id, table_id, row_id)
        connection.delete("/docs/#{doc_id}/tables/#{table_id}/rows/#{row_id}")
      end

      # Insert or Upsert a row in a specified table within a document.
      #
      # This method connects to the Coda API to insert new rows or update existing
      # rows in a specified table. The `upsert` operation is handled by the Coda
      # API itself, based on the given key columns.
      #
      # @param doc_id [String] The unique identifier of the document where the table resides.
      # @param table_id [String] The unique identifier of the table to insert or upsert rows into.
      # @param rows [Array<Hash>] An array of rows to insert or upsert, where each row is represented by a hash of column-values.
      # @option options [Array<String>] :key_columns ([]) An optional array of column IDs to uniquely identify rows for upserting.
      # @option options [Boolean] :disable_parsing (true) Determines if input parsing should be disabled—default is true.
      #
      # @return [Response] Returns the API response from the Coda service.
      #
      # @example
      #   rows = [
      #     { cells: [
      #       {
      #          "column": "c-tuVwxYz", # The column identifier.
      #          "value": "123"      # The value to be inserted into the cell.
      #        },
      #        {
      #          "column": "c-tuVwxYz", # The column identifier.
      #          "value": "123"      # The value to be inserted into the cell.
      #        }
      #       ]
      #     }
      #   ]
      #   insert_or_upsert('doc123', 'table456', rows, key_columns: ['c-tuVwxYz'])
      #
      #   returns:
      #     {
      #       "requestId": "abc-123-def-456",
      #       "addedRowIds": [
      #         "i-bCdeFgh",
      #         "i-CdEfgHi"
      #        ]
      #      }
      #
      # @see https://coda.io/developers/apis/v1#tag/Rows/operation/upsertRows
      def insert_or_upsert(doc_id, table_id, rows, key_columns = [], options = { disable_parsing: true })
        rows.each do |row|
          raise "'row' is not of the correct form - for #{row.inspect}" unless valid_row?(row)
        end
        connection.post("/docs/#{doc_id}/tables/#{table_id}/rows", query: options, body: {
          rows: rows,
          keyColumns: key_columns # Optional (Array of Coda Column IDs)
        }.to_json)
      end

      # Updates a row with the specified data.
      #
      # [updateRow](https://coda.io/developers/apis/v1#tag/Rows/operation/updateRow)
      #
      # @param row [Hash] The row object that contains the cells to be updated.
      #   The row object must conform to the following specification:
      #   {
      #     "row": {
      #       "cells": [
      #         {
      #           "column": "c-tuVwxYz", # The column identifier.
      #           "value": "$12.34"      # The value to be inserted into the cell.
      #         }
      #       ]
      #     }
      #   }
      #
      # @return [void]
      #
      # @example Update a row with a new value.
      #   update({
      #     "row": {
      #       "cells": [
      #         {
      #           "column": "c-tuVwxYz",
      #           "value": "$12.34"
      #         }
      #       ]
      #     }
      #   })
      #
      def update(doc_id, table_id, row_id, row, options = { disable_parsing: true })
        raise "'row' is not of the correct form - for #{row.inspect}" unless valid_row?(row)
        connection.put("/docs/#{doc_id}/tables/#{table_id}/rows/#{row_id}", query: options, body: { row: row }.to_json)
      end

      private

      def valid_row?(row)
        return false unless row.has_key?(:cells) && row[:cells].is_a?(Array)

        row[:cells].all? do |cell|
          cell.has_key?(:column) && cell.has_key?(:value)
        end
      end

    end
  end
end
