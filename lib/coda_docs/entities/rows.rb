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

      def insert_or_upsert(doc_id, table_id, row_id, options = { disable_parsing: true })
        connection.post("/docs/#{doc_id}/tables/#{table_id}/rows/#{row_id}", body: {
          rows: rows,
          # keyColumns: key_columns # Optional
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

        connection.put("/docs/#{doc_id}/tables/#{table_id}/rows/#{row_id}", body: { row: row }.to_json)
      end

      private

      def valid_row?(row)
        return false unless row.has_key?(:cells) && row[:cells].is_a?(Array)

        row[:cells].all? do |cell|
          cell.has_key?(:column) && cell[:column].is_a?(String) &&
          cell.has_key?(:value) && cell[:value].is_a?(String)
        end
      end

    end
  end
end
