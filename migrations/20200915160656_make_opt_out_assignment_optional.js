exports.up = function(knex) {
  return knex.schema.alterTable("opt_out", table => {
    table
      .integer("assignment_id")
      .alter()
      .nullable();
  });
};

exports.down = function(knex) {
  return knex.schema.alterTable("opt_out", table => {
    table
      .integer("assignment_id")
      .alter()
      .notNullable();
  });
};
