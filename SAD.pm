package SAD;

# 
# Self describing Ascii Database
#
# $Source: /local/www/webapps/NMR-booking/RCS/SAD.pm,v $
# $Id: SAD.pm,v 1.1 2009/06/11 11:18:22 root Exp root $
#
# Hash structure
#
#   Lists in the datastructure should be human readable.  Therefore it is made
#   sure, that the indexes start at 1
#
#  $self->{"fields"}{$field}{"index"}:            Array index of this field within DB
#                           {"options"}{$option}: Field option
#         {'fieldlist'}[]:                        Array of field names in correct order
#         {"nrofields"}:                          No. of fields in DB
#         {"formats"}{$format}:                   Output format
#         {"data"}{$key}[]:                       Array of datafields per key
#         {"index"}{$field}{}:            Hash with all entries for UNIQUE fields
#         {"flags"}{$key}{"dirty"}
#                        {"deleted"}
#                        {"added"}
#         {'config'}{'logfile'}:   Path to logfile [db_name.log]. Set to
#                                  '' to disable logging
#                   {'fieldsep'}: Separator for multiple value fields [,]
#                   {'file'}:      Path to the database
#                   {'ro'}:        read-only
#         {'web'}: Web Settings
#
#
# Database file format:
#
#  Section names and key names are case insensitive, values are case sensitive
#  the first field is always the key field and as such implies UNIQUE and REQUIRED
#
# [CONFIG]
# RECSEP = :
# NOVAL = '-'
# 
# [WEB]
# title = ACL
# admin = fthommen,carlomag,stauch,simon
# 
# 
# [FIELDS]    Fieldnames can contain alphanumeric characters, "_" and "-" (Perl: [\w-])
# 
# user: REQUIRED,AUTOINDEX
# acl: REQUIRED,FOREIGN=class@userclasses.db
# group: FOREIGN=group@groups.db
# comment
# TYPE=color
# UNIQUE
# 
# [DATA]
# alonso:admin:carlomagno:-
# amata:user:carlomagno:-
# 
#
# Supported field options are:
#  FORMAT=perl_compatible_regexp
#  REQUIRED[={yes|no}]     (not yet implemented)
#  MULTI[={yes|no}]
#  UNIQUE[={yes|no}]  # by default implies REQUIRED=yes
#  SETOF=val1[,val2[...]] (not yet implemented)
#
=pod

=head1 SAD

  SAD -- Self describing Ascii Database

=head1 SYNOPSIS

  TBD

=cut

use strict;
use Data::Dumper;

#$platform="MACOS";
my $platform="UNIX";
my %supported_sections = (fields  => 1,
                          formats => 1,
                          data    => 1,
                          web     => 1,
                          config  => 1);


my $strict = 1; # if $strict, then SAD dies at unknown sections

my %keytrans = ("field_separator"  => "fieldsep",
                "record_separator" => "recsep",
                "noval"            => "noval");

my $silent         = 0;
my $error_messages = '';
my @locked_files   = ();


sub shell {
  print "THIS IS THE SHELL\n";
  while (<>) {
    chomp;
    print $_, "\n";
    last if $_ eq 'end';
  }
  print "     B Y E\n"
}

=head1 USAGE
=cut

=over 4

=item B<new> (I<filename>[, I<flags>])

Initialize a new database object from file I<filename>.

I<flags> is a comma-separated list of flags.  Possible values for flags are

=over 2

=item *

ro (read-only)

=item *

silent (silent)

=back

=back

=cut

#
# Initialize a new DB
#
sub new {
  my ($class, $filename, $flags) = @_;
  my $self = bless {}, $class;  # create a blessed object
  my $fieldindex = 1;           # we want human readable indexes, i.e. starting at 1

  #
  # Assign defaults here
  #
  $self->{'config'}{'file'}      = $filename;
  $self->{'config'}{'logfile'}   = $filename.".log";  # Default logfile name
  $self->{'config'}{'fieldsep'} = ',';                # Default field separator
  $self->{'config'}{'recsep'} = "\t";                 # Default record separator
  $self->{'config'}{'noval'} = "-";                   # Replacement for "no data"
  $self->{'config'}{'ro'} = 0;                        # read-only flag
  $self->{'config'}{'silent'} = $flags =~ /silent/;   # silent
   $silent = $self->{'config'}{'silent'};
  $self->{"nrofields"} = 0;

  $self->{'config'}{'ro'} = $flags =~ /ro/;
  print "DB File is $filename\n";
  print "FLAGS are $flags\n";
  print "RO is $self->{'config'}{'ro'}\n";
  if (!$self->{'config'}{'ro'} && ! lockf_unix ($self->{'config'}{'file'})) {
    _print_error("Could not lock database \"".$self->{'config'}{'file'}."\"!");
    return undef;
  }

  #  open (DB, $filename) || die "ERROR opening DB \"$filename\"!\n";
  open (DB, $filename) || return undef();
  my $sect        = "";
  my $in_format   = 0;
  my $format_name = "";

  # Initialize field name array with empty element
  # as we want to have human readable indexes, i.e. starting
  # with 1
  $self->{'fieldlist'}[0] = '';

  while (<DB>) {

    if ($sect ne "formats") {
      next if /^#/; # && ($sect ne "formats");      # comment lines
      next if /^\s*$/; # && ($sect ne "formats");   # empty lines
    }
    if (/^\s*\[\s*(\w*)\s*\]\s*$/) {
      $sect = lc($1);
      if ((!$supported_sections{$sect}) && $strict) {
        die "Unknown section \"$sect\"!\n";
      }
      next;
    }

    chomp;

    SECTIONS: {

      #
      # FIELDS section
      #
      $sect eq "fields" && do {
	    # /^\s*([\w_]+)\s*(\:\s*([\w,=\s]+))?/;
        /^\s*([\w-]+)\s*(\:\s*(.+))?/;
        my $field   = $1;
        my $options = $3;
        $self->{"fields"}{$field}{"index"}   = $fieldindex++;
        push @{$self->{'fieldlist'}}, $field;

        my @options = split /,/, $options;
	    ## print "OPTIONS: $options\n" if $3;
        foreach my $opt (@options) {
	      ## print "OPTION: $opt\n" if $3;
          $opt =~ /\s*(\w*)\s*(=\s*(.*))?$/;
          my $val = $3;
	      ## print "VALUE: $3\n" if $3;
          if (lc($val) eq "yes") {$val = 1};
          if (lc($val) eq "no")  {$val = 0};
          if ($val eq "")        {$val = 1};
          $self->{"fields"}{$field}{"options"}{lc($1)} = $val;
	      # print "SETTING: $field, $1, $val\n";
        }
        $self->{"nrofields"}++;

        last SECTIONS;
      };


      #
      # FORMATS section
      #
      $sect eq "formats" && do {
       if (!$in_format) {
           next if ! /^\s*([^\s]+)/;
           $format_name = $1;
           $in_format = 1;
         } else {
           if (/^\.$/) {chomp ($self->{"format"}{$format_name}); $in_format=0; $format_name = ""; next}
           $self->{"format"}{$format_name} .= $_."\n";
         }
        last SECTIONS;
      };


      #
      # DATA section
      #
      $sect eq "data" && do {
        my @rec=(""); # prefill array with an empty element
        my $rsep  = $self->{'config'}{'recsep'};
        my $noval = $self->{'config'}{'noval'};
        # push @rec, split (/\s*$rsep\s*/, $_);
        push @rec, split (/$rsep/, $_);
        my $key = $rec[1];

        # check for empty key
        if ($key eq $noval || $key =~ /^\s*$/) {
          _print_error ("Empty keys are not allowed");
          _print_error ("  *** RECORD ".join($rsep, @rec)." WILL BE IGNORED ***");
          last SECTIONS;
        }

        # check for duplicate keys
        if (exists $self->{"data"}{$key}) {
          _print_error ("Duplicate key $key");
          _print_error ("  Already in database as ".join($rsep, @{$self->{"data"}{$rec[1]}}));
          _print_error ("  *** IGNORING THIS ENTRY ***");
          last SECTIONS;
        }
               
        my $nrofrecs = $#rec;
        # print "NR OF RECORDS: $nrofrecs\n";
        for (my $i=1; $i<= $nrofrecs; $i++) {
          my $field = $self->{"fieldlist"}[$i];
          print "CHECKING FIELD $field\n";
          $rec[$i]  = "" if $rec[$i] eq $noval;
          # print "RECORD ITEM Nr $i is $rec[$i]\n";
          # print "NO VALUE at $i\n" if  $rec[$i] eq $noval;
          
          # check for duplicate values in UNIQUE fields
          if ( $self->has_option($field, "unique") ) {
            if ( !($rec[$i] eq "" &&
                   !($self->has_option($field, "required")) )
               ) {
              print "NOW CHECKING Field $field FOR DUPLICATES ($key)\n";
              if ( exists($self->{"index"}{$field}{$rec[$i]}) ) {
                _print_error ("Duplicate value \"$rec[$i]\" for field \"$field\" in record $rec[1]");
                my @duprecs = $self->search($field, '^'.$rec[$i].'$');
                _print_error ("Same value also appears in records \"".join (", ", @duprecs)."\"");
                _print_error ("*** PLEASE CORRECT MANUALLY ***");
                abort();
              }
              $self->{"index"}{$field}{$rec[$i]} = 1;  # add entry in index of this field
            }
          }
        }
             
        @{$self->{"data"}{$rec[1]}} = @rec;
        if ($nrofrecs != $self->{"nrofields"}) {
           _print_error ("Unequal Element number (Record $rec[1]: got " . $#rec . ", expected $self->{nrofields})");
           print "  FIELDS:\n";
           for (my $i=1; $i<= $nrofrecs; $i++) {
             print "   $i ($self->{'fieldlist'}[$i]): $rec[$i]\n";
           }
           $strict && abort();
        }
        # print "\n";
        last SECTIONS;
        };

      #
      # CONFIG section
      #
      $sect eq "config" && do {
        my ($key, $dummy, $val) = $_ =~ / *(\w*)( *= *(.*))? *$/;
        $key = lc($key);
        if (exists $keytrans{$key}) {$key = $keytrans{$key}}
        $val  = _dequote ($val);
        $self->{'config'}{$key} = $val;
        # print "CONFIG \'$key\' is \'$val\'\n";
        last SECTIONS;
        };

      #
      # web section
      #
      $sect eq "web" && do {
        my ($key, $val) = $_ =~ / *(\w*) *= *(.*) *$/;
        $key = lc($key);
        if (exists $keytrans{$key}) {$key = $keytrans{$key}}
        $val  = _dequote ($val);
        $self->{'web'}{$key} = $val;
        last SECTIONS;
        };
    }

  }  # while <DB>
  close (DB);

  return $self;
}

#
# Switch DB mode ro/rw
#
sub switch_mode {
  my ($self, $new_mode) = @_;
  my $current_mode = 'rw';
  $current_mode = 'ro' if $self->{'config'}{'ro'};
  if ($current_mode eq $new_mode) {return 1};
  if (($current_mode eq 'ro') && ($new_mode eq 'rw')) {
    if (!lockf_unix ($self->{'config'}{'file'})) {
      _print_error('[switch_mode] Database could not be locked');
      return 0
    } else {
      $self->{'config'}{'ro'} = 0;
      return 1
    }
  }
}




#############################################

sub insert_column {
    my ($self, $name, $pos, $options) = @_;

    print "field ORDER is now (BEFORE):\n";
    foreach my $i (@{$self->{'fieldlist'}}) { print "  * $i (", $self->{'fields'}{$i}{'index'}, ")\n"}
    print "\n";

    # adjust number of fields
    $self->{'nrofields'}++;

    # adjust index of each field and add index for new field
    foreach my $f (keys %{$self->{'fields'}}) {
	if ($self->{'fields'}{$f}{'index'} >= $pos) {
          $self->{'fields'}{$f}{'index'}++
        }
    }
    $self->{'fields'}{$name}{'index'} = $pos;



    print "fields are IN BETWEEN:\n";
    foreach my $f (keys %{$self->{'fields'}}) {print "  * $f ->  $self->{'fields'}{$f}{'index'}\n"};



    # adjust field list
    foreach my $f (keys %{$self->{'fields'}}) {
        my $i = $self->{'fields'}{$f}{'index'};
	$self->{'fieldlist'}[$i] = $f
    }

    $self->{'config'}{'rebuild'} = $name;
    print "INSERTED FIELD $name at POS $pos\n";
    print "field ORDER is now:\n";
    foreach my $i (@{$self->{'fieldlist'}}) { print "  * $i (", $self->{'fields'}{$i}{'index'}, ")\n"}
    print "\n";
}

#############################################


sub _dequote {
  my ($val) = @_;
  ($val =~ /^'(.*)'$/) && ($val = $1);
  ($val =~ /^"(.*)"$/) && ($val = $1);
  return $val
}



#
# Status functions
#
sub is_multivalue {
  my ($self, $field) = @_;
  return $self->has_option($field, "multi");
}

sub is_required {
  my ($self, $field) = @_;
  return $self->has_option($field, "required");
}

sub error_messages {
  return $error_messages
}


sub printdb {
  my ($self, $key) = @_;

  $| = 1;

  print STDERR "Printing Keys ($key)\n";

  if ($key) {
    print STDERR $self->{"data"}{$key}[1], "\n";
  } else {
    foreach my $k (keys %{$self->{"data"}}) {
      print STDERR "KEY: $k\n";
    }
  }

  print STDERR "Formats\n\n";
  foreach my $k (keys %{$self->{"format"}}) {
    print STDERR "**", $k, "**\n";
    print STDERR $self->{"format"}{$k};
  }
}



sub checkentry {
  my ($self, $key, $field, $val) = @_;
  my $format = $self->{"fields"}{$field}{"options"}{"format"};
  my $required = $self->has_option($field, "required");

  #
  # Check if record exists
  #
  if (! exists $self->{"data"}{$key}) {
    _print_error ("[checkentry] No such record \'$key\'!");
    return 0;
  }

  #
  # check if field exists
  #
  if (! $self->{"fields"}{$field}{"index"}) {
    _print_error ("[checkentry] No such field \'$field\'.");
    return 0;
  }

  #
  # Check for record separator in content
  #
  if ($val =~ /.*$self->{"config"}{"recsep"}.*/) {
    _print_error ("[checkentry] Record separator (\"$self->{'config'}{'recsep'}\") is not allowed in field contents (occurred in field \'$field\').");
    return 0
  }

  #
  # Check options
  #
  if ($required && !$val) {
    _print_error ("[checkentry] Required field \'$field\'.");
    return 0
  }
  $format && do {
    ## print "FORMAT: $format\n";
    if ($val !~ /$format/) {
      _print_error ("[checkentry] WRONG FORMAT ($key: $field, $val) [$format].");
      return 0
    }
  };

  return 1;
}

#
# get - return single entry from specific record
#
sub get {
  my ($self, $key, $field) = @_;
  # test this for autoviviation!!!
  my $index = $self->{"fields"}{$field}{"index"};
  my $sep = $self->{'config'}{'fieldsep'};

  if ( ! exists $self->{"data"}{$key} ) {
       return undef();
  } else {
    if (wantarray() && $self->is_multivalue($field)) {
      return split /\s*$sep\s*/, $self->{'data'}{$key}[$index];
    } else {
      return $self->{"data"}{$key}[$index];
    }
  }
}


#
# get_record - return specific record
#
sub get_record {
  my ($self, $key) = @_;
  my %rec          = ();

  foreach  ( keys %{$self->{"fields"}} ) {
    my $index = $self->{"fields"}{$_}{"index"};
    $rec{$_}  = $self->{"data"}{$key}[$index];
    if ($rec{$_} eq $self->{'config'}{'noval'} ) {$rec{$_} = ''};
  }

  return %rec;
}



sub contains {
  my ($self, $key, $field, $string) = @_;
  my $index  = $self->{"fields"}{$field}{"index"};
  my $sep    = $self->{'config'}{'fieldsep'};
  my $found  = 0;
  my @fields = $self->get($key, $field);

  foreach (@fields) {
    $found=1 if $_ eq $string;
    return $found if $found;
  }
  return $found;
}



#
# QUERY the database
#
#  query (query, [keys]);
#     query: <field>=<value>[:<field>=<value>[...]].  Queries are ORed
#            if no queries are given, then all keys are returned
#     keys: Single key or list of keys to query. Empty if all keys
#           should be queried
#
sub query {
  my ($self, $query, @keys) = @_;
  my @result = ();
  my @qkeys = ();
  my @queries = split /:/, $query;
  my @query = ();
  my $found = 0;

  #  foreach $k (@queries) {
  #    my @k = split /=/, $k;
  ##    foreach $a (@k) {print "   -> $a\n";}
  #    $query[++$#query] = [@k];
  ##    print "QUERY NUMBER: $#query\n";
  #  }
  # print "***", $_[0], "<br>";
  # print "***", $_[1], "<br>";
  # print "***", $_[2], "<br>";
  # print "QUERY: $query\n";
  # print "KEYS: @keys\n";

  if (@keys) {
    @qkeys = sort @keys;
  } else {
    @qkeys = listkeys($self);
  }

  # print "NO KEYS GIVEN" if !@keys;;

  if (!@queries) {
    return @qkeys
  }

  # print "C O N T I N U E\n";

  foreach my $k (@qkeys) {        # foreach key
    $found = 0;
    foreach my $q (@queries) {    # foreach query
      last if $found;
      my @q = split /=/, $q;
      my @v = $self->get($k, $q[0]);
      foreach my $v (@v) {     # foreach subfield
        if ($v eq  $q[1]) {
        # print "DEBUG: $v - $q[1] - $k\n";
           push @result, $k;
           $found = 1;
           last;
        }
      }

    }
  }
  if (@result) {return @result}
  else         {return ()}
}


#
# set: Change a single field in a record
#
sub set {
  my ($self, $key, $field, $val) = @_;

  if ($self->{'config'}{'ro'}) {
    _print_error ("[set] Cannot change values.  Database $self->{'config'}{'file'} locked\n");
    return 0;
  };


  if (!$self->checkentry($key, $field, $val)) {
    _print_error ("[set] Errors in entry check.  Record \'$key\' unchanged!\n");
    return 0;
  };

  $self->{"data"}{$key}[$self->{"fields"}{$field}{"index"}] = $val;
  if ($self->{"flags"}{$key}{"dirty"}) {
    $self->{"flags"}{$key}{"dirty"} .=";";
  }
  $self->{"flags"}{$key}{"dirty"} .= "$field=$val";
  return 1;
}


#
# add_to: Add a value to a multivalue field
#
sub add_to {
  my ($self, $key, $field, $val) = @_;
  my $res = 0;
  my $sep = $self->{'config'}{'fieldsep'};

  if ($self->{'config'}{'ro'}) {
    _print_error ("[add_to] Cannot change values.  Database $self->{'config'}{'file'} locked\n");
    return 0;
  };

  if (!$self->is_multivalue($field)) {
    print STDERR "ERROR: Field $field is not multivalued!\n";
    return 0;
  }

  if (!$self->contains($key, $field, $val)) {
    $res = $self->set($key, $field, $self->$field($key).$sep.$val)
  }

  return $res;
}


sub remove_from {
  my ($self, $key, $field, $val) = @_;
  my $res = 0;
  my $sep = $self->{'config'}{'fieldsep'};

  if ($self->{'config'}{'ro'}) {
    _print_error ("Cannot change values.  Database $self->{'config'}{'file'} locked\n");
    return 0;
  };

  if (!$self->is_multivalue($field)) {
    print STDERR "ERROR: Field $field is not multivalued!\n";
    return 0;
  }

  my @vals  = $self->get($key, $field);
  my @vals2 = ();
  foreach (my $i=0 ; $i<=$#vals ; $i++) {
    if ($vals[$i] ne $val) {
      push (@vals2, $vals[$i]);
    }
  }

  $res = $self->set($key, $field, join($sep, @vals2));

  return $res;
}


sub record_exists {
  my ($self, $key) = @_;
  return exists $self->{'data'}{$key};
} 

sub field_exists {
  my ($self, $field) = @_;
  return exists $self->{'fields'}{$field};
}

sub has_option {
  my ($self, $field, $option) = @_;
  $option = lc($option);
  
  # return exists $self->{'fields'}{$field}{'options'}{$option};
  # be careful not to directly test for $self->{'fields'}{$field}{'options'}{$option}
  # as this could autovivify $self->{'fields'}{$field} for a not existing $field e.g when
  # - by error - looping through all indices instead of starting at 1 :-)
  #
  # return 1;
  # print Dumper($self->{'fields'}{$field});
  if (exists $self->{'fields'}{$field} &&
      $self->{'fields'}{$field}{'options'}{$option}) {return 1}
  else                                               {return 0}
}


sub get_option {
  my ($self, $field, $option) = @_;
  $option = $self->{'fields'}{$field}{'options'}{lc($option)};

  if (wantarray()) {
    return split(/\|/, $option)
  } else {
    return $option;
  }
}


sub listfields {
  my ($self) = @_;
  my @fieldlist = @{$self->{'fieldlist'}};

  shift @fieldlist;
  return @fieldlist
}


sub add_record {
  my ($self, $key, %data) = @_;
#  my $autoindex = $self->{"fields"}{$self->{"fieldlist"}[1]}{"options"}{"autoindex"};
  my $autoindex = $self->has_option($self->{"fieldlist"}[1], "autoindex");
  #    _print_error ("New key is $key, $autoindex, ".$self->{"fieldlist"}[1].", ". $self->{"fields"}{$self->{"fieldlist"}[1]}{"options"}{"AUTOINDEX"});
  #    return 0;
  if ($autoindex) {
    my @i = sort ({$a <=>$b} $self->listkeys());
    my $maxindex = $i[$#i] += 1;
    $key = $maxindex;
    _print_error ("New key is $key");
    # return 0;
  }

  if (!$key) {
    _print_error ("[add_record] Must define a key for new records!");
    return 0;
  }

  #
  # Check if record already exists
  #
  if (exists $self->{"data"}{$key}) {
    _print_error ("[add_record] Record already exists \'$key\'!");
    return 0;
  }
  @{$self->{"data"}{$key}} = ();  # Initialize new record with empty array


  #
  # Add datafields and check if record complete, else delete
  #
  my $ok = 1;
  my $keyfield = $self->{'fieldlist'}[1];

  # key first
  $ok = $self->set($key, $keyfield, $key) && $ok;

  foreach my $k ($self->listfields()) {
    next if $keyfield eq $k;  # ignore keyfield overwrites
    $ok = $self->set($key, $k, $data{$k}) && $ok;
  }

  if (! $ok) {
    _print_error ("[add_record] New record \'$key\' has errors.  IGNORED.");
    delete ($self->{"data"}{$key});
    return 0;
  }

  $self->{"flags"}{$key}{"added"} = 1;
  return 1;
}


#
# search: returns a list of all keys of records, that match $re in $field
#
sub search {
  my ($self, $field, $re, $mykeys) = @_;
  my @mykeys = ();
  if ($mykeys) {
    @mykeys = @$mykeys
  } else {
    @mykeys = keys %{$self->{"data"}}
  }
  my @result = ();

  foreach my $k (@mykeys) {
    push @result, $k if $self->{"data"}{$k}[$self->{"fields"}{$field}{"index"}] =~ /$re/;
  }

  return @result;
}

#
# esearch: returns a list of all keys of records, that match $re in $field
#          with comparison operator $op
#
sub esearch {
  my ($self, $field, $op, $re, $mykeys) = @_;
  my @mykeys = ();
  if ($mykeys) {
    @mykeys = @$mykeys
  } else {
    @mykeys = keys %{$self->{"data"}}
  }
  my @result = ();

  foreach my $k (@mykeys) {
    my $mykey = $self->{"data"}{$k}[$self->{"fields"}{$field}{"index"}];
    $mykey = '"'.$mykey.'"';
    push @result, $k if eval("$mykey $op /$re/");
  }

  return @result;
}



#
# listkeys: Returns a list of all keys within the database
#
sub listkeys {
  my ($self) = @_;
  my @result = ();

  #  foreach my $k (keys %{$self->{"data"}}) {
  #    push @result, $k;
  #  }

  return sort keys %{$self->{"data"}};
}


#
# listsortedkeys: Returns a sorted list of all keys within the database
#
sub listkeyssorted {
  my ($self, $sortfield, $mykeys) = @_;
  my @mykeys = ();
  if ($mykeys) {
    @mykeys = @$mykeys
  } else {
    @mykeys  = keys %{$self->{"data"}};
  }
  my @result = ();

  if ( ! $self->field_exists($sortfield) ) {print "SAD ERROR: No such field known: '$sortfield'\n"; exit}

  #  foreach my $k (keys %{$self->{"data"}}) {
  #    push @result, $k;
  #  }

  return sort { $self->{"data"}{$a}[$self->{"fields"}{$sortfield}{"index"}] cmp $self->{"data"}{$b}[$self->{"fields"}{$sortfield}{"index"}] } @mykeys;
}


my $timeout  = 10;
my $lock_ext = ".lock";

sub lockf_unix {
  my ($file) = @_;
  my $wait = 0;

  return 1 if $platform eq "MACOS";

  while (! link $file, $file.$lock_ext) {
    print "LOCKING $file, $file.$lock_ext\n";
    sleep 1; $wait++;
    if ($wait > $timeout) {return 0}
  }
  push @locked_files, $file;
  return 1;
}

# copied from listadmin
sub lockf_mac {
  my ($file) = @_;
  my $wait = 0;
  local *F;

  while (-e $file.$lock_ext) {
    sleep 1; $wait++;
    if ($wait > $timeout) {&add_errmsg("Could not lock $file"); return 0}
  }
  open (F, ">$file$lock_ext") || do {&add_errmsg("Could not lock $file"); return 0};
#  open (F, ">$file$lock_ext") || return 0;
  push @locked_files, "$file";
  close (F);
  return 1;
}


sub unlockf {
  my @files = @_;
  foreach my $f (@files) {
    unlink "$f"."$lock_ext";
  }
}


sub abort {
  my ($self) = @_;
  unlockf (@locked_files);
  _print_error("Aborting operation\n");
#  $self->{} = {};
#  SAD::DESTROY();
  exit;
}

sub DESTROY {
  unlockf (@locked_files);
}


sub substitute {
  my ($self, $key, $name, $no) = @_;
  my $string = $self->{"data"}{$key}[$self->{"fields"}{$name}{"index"}];

  $no = $no ? $no - length($string) : 0 ;
  return $string . " " x $no
}


#
# write: write the record with key $key in format $format to STDOUT
#
sub write {
  my ($self, $key, $format) = @_;

  #
  # Check if record exists
  #
  if (! exists $self->{"data"}{$key}) {
    _print_error ("No such record \'$key\' (write)!");
    return 0;
  }

  my $line = $self->{"format"}{$format} || do{_print_error("Format \'$format\' not found"); exit};

  #  $line =~ s/\$\{([^\s}]+)\}/$self->{"data"}{$key}[$self->{"fields"}{$1}{"index"}]/g;
  #  $line =~ s/\$([^\s]+)/$self->{"data"}{$key}[$self->{"fields"}{$1}{"index"}]/g;
  $line =~ s/\$\{([^\s}]+)\}(\[([\d]*)\])?/$self->substitute($key, $1, $3)/ge;
  $line =~ s/\$([^\s]+)/$self->substitute($key, $1)/ge;
  print $line, "\n";
  return 1;
}


#
# update: change multiple values at once??????
#
#
# !!! Consistency check
# !!! multiple sets not yet implemented
#
sub update {
  my ($self, $key, $datastring) = @_;
  my ($val, $data) = split /=/, $datastring;
  my $format = $self->{"fields"}{$val}{"options"}{"format"};


  if ($self->{'config'}{'ro'}) {
    _print_error ("Cannot change values.  Database $self->{'config'}{'file'} locked\n");
    return 0;
  };

  $format && do {
    if ($data !~ /$format/) {
      _print_error ("WRONG FORMAT ($key: $datastring, $data) [$format]");
      return 0
    }
  };

  $self->{"data"}{$key}[$self->{"fields"}{$val}{"index"}] = $data;
  if ($self->{"flags"}{$key}{"dirty"}) {
    $self->{"flags"}{$key}{"dirty"} .=";$datastring";
  } else {
    $self->{"flags"}{$key}{"dirty"} .= "$datastring";
  }
  return 1;
}



sub delete_record {
  my ($self, $key) = @_;

  if ($self->{'config'}{'ro'}) {
    _print_error ("Cannot delete keys.  Database $self->{'config'}{'file'} is read-only\n");
    return 0;
  };

  $self->checkentry($key);
  delete $self->{"data"}{$key};
  $self->{"flags"}{$key}{"dirty"}   = 0;
  $self->{"flags"}{$key}{"deleted"} = 1;

  return 1;
}


sub commit {
  my ($self) = @_;
  my $log = 1;

  my $rsep = $self->{'config'}{'recsep'};

  my $input   = $self->{'config'}{'file'};
  my $output  = "$self->{'config'}{'file'}".".tmp";
  my $logfile = $self->{'config'}{'logfile'};

  die "Database $self->{'config'}{'file'} is READ-ONLY!\n"
     if $self->{'config'}{'ro'};

  open (OUT, ">$output") || die "Unable to open database OUT ($output)!\n";
  open (IN, $input) || do {close(OUT); die "Unable to open database IN ($input)!\n"};
  if ($logfile && -e "$logfile") {
    open (LOG, ">>$logfile") || do {close (OUT); close (IN); die "Unable to open LOG ($logfile)\n"};
    $log = 1;
  }


  # Read old database from file and write back until the [data] section starts

  my $rebuild = $self->{'config'}{'rebuild'};
  my @fieldsdone = ();
  my $f_ind = 0;

  while (<IN>) {
    chomp;
    # print OUT $_."\n";
    /^\s*\[\s*(\w*)\s*\]\s*$/;
    my $sect = lc($1);
    close (IN) if ($sect eq "data");
    if ($rebuild && ($sect eq "fields")) {
      ### @line = $_;
      if (/^\s*([\w_]+)\s*(\:\s*(.+))?/) {
        my $field = $1; my $options = $3;
        if ($self->{'fieldlist'}[$f_ind+1] eq $field) {
	  print "printed FIELD $field in CORRECT POSITION ", $f_ind+1, "\n";
	  print OUT $_."\n"; $f_ind++; # unmodified field line
        } else {
	   print "printed NEW FIELD $field in NEW POSITION ", $f_ind+1, " (", $self->{'fieldlist'}[$f_ind], ")\n";
	   print OUT "$self->{'fieldlist'}[$f_ind]\n";  $f_ind++;
        }
      } else {
        print OUT $_."\n";
      }   
    } else {
      print OUT $_."\n";
    }
}

  # log added records

  foreach my $key (keys %{$self->{"flags"}}) {
    if ($self->{"flags"}{$key}{"added"}) {
      $log && _print_log(\*LOG, "Added Entry for key \'$key\'");
    }
  }


  # write back data

  foreach my $key (sort keys %{$self->{'data'}}) {
    if ($self->{"flags"}{$key}{"dirty"}) {
       $log && _print_log(\*LOG, "Modified Data for key $key, ", $self->{"flags"}{$key}{"dirty"});
       $self->{"flags"}{$key}{"dirty"} = 0;  # clean "dirty" flag
    }
    foreach my $field (keys %{$self->{'fields'}}) {
      if (! $self->{'data'}{$key}[$self->{"fields"}{$field}{"index"}]) {
        $self->{'data'}{$key}[$self->{"fields"}{$field}{"index"}] = '-';
      }
    }
    # now shift the array for printout (remember: our "real"
    # list starts at index 1, not 0, i.e. the first element
    # in the list is empty an unused)
    # BUT
    # we may NOT shift the internal array used for
    # database access
    my @output_data = @{$self->{'data'}{$key}};
    shift @output_data;
    print OUT join($rsep, @output_data), "\n";
  }
  foreach my $key (keys %{$self->{"flags"}}) {
    if ($self->{"flags"}{$key}{"deleted"}) {
      $log && _print_log(\*LOG, "Deleted Entry for key \'$key\'");
    }
  }
  $log && _print_log(\*LOG, "End Transaction");
  close (LOG) if $log;
  close (OUT);
  rename ($output, $input);
}


sub closedb {
  my ($self) = @_;
  $self->commit();
  unlockf();
}



#------------------------

sub dumpconfig {
  my ($self) = @_;
  
  print "Configuration dump\n\n";

  print "General:\n";
  foreach (keys %{$self->{'config'}}) {
    print " * $_ = ", $self->{'config'}{$_}, "\n";
  }

  print "\nFields:\n";
  foreach ($self->listfields()) {
    print " * [$self->{'fields'}{$_}{'index'}] $_: ";
    my @options;
    foreach my $o (keys %{$self->{'fields'}{$_}{'options'}}) {
      push @options, "$o=$self->{'fields'}{$_}{'options'}{$o}"
    }
    print join(", ", @options);
    print "\n";
  }

  print Dumper($self);
}


#------------------------------


sub _print_error {
  my ($msg) = @_;
  if ($silent) {
    $error_messages .= "$msg\n"
  } else {
    print STDERR "*** ERROR -- $msg\n";
  }
}

sub _print_log {
  my ($fh, $msg) = @_;
  print $fh localtime().": $msg\n";
}


# very,very simplicistic admin test
sub isadmin {
  my ($self) = @_;
  my @admins = split (/\s*,\s*/, $self->{'web'}{'admin'});
  foreach (@admins) {
    return 1 if $_ eq $ENV{'REMOTE_USER'};
  }
  return 0
}

#
# AUTOLOAD: Return a field value if key is given as command
#
sub AUTOLOAD {
  my ($self, @args) = @_;
  my $AUTOLOAD =~ /::(\w+)$/;
  if (defined $self->{'data'}{$args[0]}) {
    return $self->get($args[0], $1)
  } else {
    return undef;
    # SUPER::AUTOLOAD
  }
}


my $Interrupted = 0;   # to ensure it has a value

$SIG{INT} = sub {
  $Interrupted++;
  syswrite(STDERR, "ouch\n", 5);
  SAD::abort();
};


1;


###########################################################################
#
#   Package SAD::CGI for CGI functions
#
#
#


package SAD::CGI;

use SAD;
use CGI;
use strict;


sub print_html_table {
  my ($self, $action) = @_;
  my $admin  = (($action eq 'admin') && $self->isadmin());
  my $color  = '';
  my $title  = $self->{'web'}{'title'};
  my $dbname = '';

#  print "<p>PATH_INFO: $ENV{'PATH_INFO'}\n"; 
#  print "<p>PATH_TRANSLATED: $ENV{'PATH_TRANSLATED'}\n";

#  if ($admin && !$ENV{'REMOTE_USER'}) {
#    print "<h1>You are not logged in</h1>";
#    exit
#  }

  if (CGI::param('db')) {
    $dbname = CGI::param('db');
    CGI::delete('db')
  }
  if ($ENV{'PATH_INFO'}) {$dbname = ''};

  my $sortkey = CGI::param('sad_sortby');
  $sortkey = $sortkey ? $sortkey : $self->{'fieldlist'}[1];

  my $url = CGI::url(-path_info => 1, -query_string => 1);
  $url = $url.'/'.$dbname if $dbname;

  my @fieldlist = @{$self->{'fieldlist'}};
  shift @fieldlist;

  #
  # Search records
  #
  my @all_keys = $self->listkeys();
  foreach (@fieldlist) {
    my $p = CGI::param($_);
    @all_keys = $self->search($_, $p, \@all_keys) if $p;
  }
  my $nr_found = @all_keys;

  
  my @all_keys = $self->listkeyssorted($sortkey, \@all_keys);
  my $tablewidth = $self->{'nrofields'}+1;


  print "<h1 align=\"center\">$title</h1>\n";
  if (!$admin && exists $self->{config}->{'adminonly'}) {
      print "<h2 align=\"center\">Sorry, this database is only accessible for database administrators</h2>\n";
      return;
  }
  
  
  print "<p>Sorted by $sortkey, Current set: $nr_found entries</p>\n";
  if ($admin) {
    my $url = CGI::url(-path_info => 1);
    print "<a href=\"$url?sad_action=new\">Add new entry</a>\n";
  }
  print "<p>Status: $action, ", $self->isadmin(), "\n";
  print "<p>Last database modification: ".localtime((stat($self->{'config'}{'file'}))[9])."\n";
  print "<p>Logged in as ", $ENV{'REMOTE_USER'}, "\n";
  print "<p>Click on table headers to sort</p>\n";

  #
  # Search form at the beginning of the table
  #
  print CGI::start_form({method=>'get',action=>$url});
  print CGI::hidden('sad_sortby', $sortkey), "\n";;
  print "<table border=\"0\">\n";

  print "<tr>\n  ";
  foreach my $head (@fieldlist) {
    print "<th>$head</th>"
  }
  print "<th>&nbsp;</th>\n";
  print "</tr>\n";


  print "<tr>\n  ";
  foreach my $head (@fieldlist) {
    if ($self->has_option($head, 'restrict')) {
      my @valuelist = $self->get_option($head, 'restrict');
      unshift @valuelist, "";
      print "<td>", CGI::popup_menu($head, \@valuelist)  ,"</td>\n";
    } else {
      print "<td>", CGI::textfield($head, '', 5). "</td>\n";
    }
  }
  print "<th>", CGI::submit('Search'), "</th>\n";
  print "</tr>\n";

  print CGI::end_form();

  print "<tr><td class=\"noborder\" colspan=\"$tablewidth\">&nbsp;</td></tr>\n";
  #  print "</table>\n";

  #  print "<table border=\"0\">\n";
  print "<tr>\n  ";


  #
  # Table header with sort links
  #
  foreach my $head (@fieldlist) {
    my $color = '';
    my $link = '';
    if ($head eq $sortkey) {
      $color = 'bgcolor="silver"';
      $link  = $head;
    } else {
      CGI::param('sad_sortby', $head);
      my $url = CGI::url(-path_info => 1);
         $url = $url.'/'.$dbname if $dbname;
      $link = "<a href=\"$url?".CGI::query_string()."\">$head</a>";
    }
    print "<th $color>$link</th>"
  }
  if ($admin) {print "<th $color>Admin</th>"};
  print "</tr>\n";


  #
  # Table content
  #
  foreach my $key (@all_keys) {

    foreach my $field (keys %{$self->{'fields'}}) {
      my $index = $self->{"fields"}{$field}{"index"};
      if (! $self->{'data'}{$key}[$index]) {
        $self->{'data'}{$key}[$index] = '-';
      }
      
      if ($self->{"fields"}{$field}{"options"}{"type"} == "color") {
	    # $color="bgcolor=\"$self->{'data'}{$key}[$index]\"";
	    # $color="bgcolor=\"silver\"";
      } else {
	    # $color=''
      }
    }
    if ($self->{"flags"}{$key}{"dirty"}) {
      $color = 'bgcolor="red"'
    } else {
      $color=''
    }
    
    print "<tr $color><td>";
    my @output_data = @{$self->{'data'}{$key}};
    foreach my $f ($self->{'fieldlist'}) {
       if ($self->has_option($f, 'autolink')) {
         my $link = ($self->get_option($f, 'autolink') =~ s/%/$key/);
         $link = $self->get_option($f, 'autolink');
         $output_data[$self->{'fields'}{$f}{1}] = $link;
       }
    }
    if ($admin) {
      my $url = CGI::url(-path_info => 1);
      CGI::delete_all();
      push @output_data, "<a href=\"$url?sad_action=delete&sad_delkey=$key\">delete</a> | <a href=\"$url?sad_action=edit&sad_editkey=$key\">edit</a>";
    }
    shift @output_data;
    print join('</td><td>', @output_data);
    print "</td></tr>\n";
  }
  print "</table>\n";
  print "<br>\n";

  print "</body>\n</html>\n";
}



=pod


=head2 B<print_edit_form>


Prints an input form with a table to edit any regular field of the
database B<besides> the key field

=cut


sub print_edit_form {
  my ($self, $key) = @_;
  my @fieldlist = $self->listfields();
  my $keyfield = $self->{'fieldlist'}[1];
  my $title  = $self->{'web'}{'title'};
  my $css = '<link href="/NMR-booking/booking.css" rel="stylesheet" type="text/css">';


  print "<html>\n<head>\n<title>AAA$title</title>\n$css\n</head>\n<body>\n";
  print "<h2 align=\"center\">Editing record with <code>$keyfield=", CGI::param('sad_editkey'), "</code></h2>\n";

  print "<table border=\"1\">\n";

  print CGI::start_form({method=>'get'});

  print "<tr><td><b>Field</b></td><td><b>Value</b></td></tr>\n";
  foreach my $field (@fieldlist) {
    next if ($keyfield eq $field);  # skip key field
    print "<tr>\n";
    print "  <td>$field</td>\n";
    if ($self->has_option($field, 'restrict')) {
      my @valuelist = $self->get_option($field, 'restrict');
      print "  <td>", CGI::popup_menu($field, \@valuelist, $self->$field($key)), "</td>\n";
    } else {
      # force scalar, otherwise multivalue field will only have the first entry in the form
      print "  <td>", CGI::textfield($field, scalar($self->$field($key))), "</td>\n";
    }
    print "</tr>\n"
  }

  print "</table>\n";
  print CGI::hidden($keyfield, $key);
  print CGI::defaults();
  print CGI::submit('sad_action', 'Submit');
  print CGI::end_form();

}


sub print_submit_confirm {
  my ($self, $key) = @_;
  my @fieldlist = @{$self->{'fieldlist'}}; shift @fieldlist;
  my $title  = $self->{'web'}{'title'};
  my $css = '<link href="/NMR-booking/booking.css" rel="stylesheet" type="text/css">';

  print "<html>\n<head>\n<title>$title</title>\n$css\n</head>\n<body>\n";
  print "<h1 align=\"center\">Confirm modifications</h1>\n";

  print "<table border=\"1\">\n";
  print CGI::start_form({method=>'get'});

  print "<tr><td><b>Field</b></td><td><b>Current Value</b></td><td><b>New Value</b></td></tr>\n";

  foreach my $field (@fieldlist) {
    my $current_val = $self->$field($key);
    my $new_val     = CGI::param($field);
    print CGI::hidden($field, $new_val), "\n";
    if ($current_val ne $new_val) {
      $new_val = CGI::b($new_val);
    }
    print "<tr>\n";
    print "  <td>$field</td>\n";
    print "  <td>$current_val</td>\n";
    print "  <td>$new_val</td>\n";
    print "</tr>\n"
  }

  print "</table>\n";
  print CGI::defaults();
  print CGI::submit('sad_action', 'Really Submit');
  print CGI::end_form();

}

=pod


=head2 B<print_new_entry_form>


Prints an input form for adding a new entry.

=cut



sub print_new_entry_form {
  my ($self) = @_;
  my @fieldlist = $self->listfields();
  my $keyfield  = $self->{'fieldlist'}[1];
  my $css = '<link href="/NMR-booking/booking.css" rel="stylesheet" type="text/css">';
  my $title  = $self->{'web'}{'title'};


  print "<html>\n<head>\n<title>$title</title>\n$css\n</head>\n<body>\n";
  print "<h2 align=\"center\">Adding new record to database</h2>\n";

  print "<table border=\"1\">\n";

  print CGI::start_form({method=>'get'});

  print "<tr><td><b>Field</b></td><td><b>Value</b></td></tr>\n";
  foreach my $field (@fieldlist) {
    # my $f = "<b>$field</b>" if $field eq $keyfield;  # mark key field
    print "<tr>\n";
    print "  <td>$field</td>\n";
    print "  <td>", CGI::textfield($field, ''), "</td>\n";
    print "</tr>\n"
  }

  print "</table>\n";
  print CGI::defaults();
  print CGI::submit('sad_action', 'Add');
  print CGI::end_form();

}


=pod

=head1 CGI

CGI Dokumentation

=cut


sub cgi {
  my ($db, $inline, $mode) = @_;
  # $mode = 'user' if !isadmin();
  my $dbname = '';
  my $css = '<link href="/NMR-booking/booking.css" rel="stylesheet" type="text/css">';

  # Get the name of the database
  if (ref($db) eq 'HASH') {
    $dbname = $ENV{'PATH_INFO'};
    $dbname =~ /^\/([^\/]+)/;
    $dbname = $1;
    if (CGI::param('db')) { $dbname = CGI::param('db') };
  } else {
    $dbname = $db;
  }

  ## DEBUG ## print "<h1>$dbname, $ENV{'PATH_INFO'}</h1>\n";

  # Any action pending?
  my $action = lc(CGI::param('sad_action'));
  my $table  = SAD->new($db->{$dbname}, 'ro');
  $mode = 'user' if !$table->isadmin();
  
  my $title  = $table->{'web'}{'title'};
  print "Content-Type: text/html\n\n";
  print "<html>\n<head>\n<title>$title</title>\n$css\n</head>\n<body>\n";

  if ($action && !($mode eq 'admin')) {
    print "<h1>Not Admin, $action, $mode, $dbname</h1>\n";
    # my $table  = SAD->new($db->{$dbname});
    $table->SAD::CGI::print_html_table($mode);
    exit;
  }


  #
  # Delete entries
  #
  if ($action eq 'delete') {
    if (!CGI::param('sad_delkey')) {
      print "<h2>No Delete Key given</h2>\n";
    } else {
       print "<h2>Delete Key ".CGI::param('sasd_delkey')."</h2>\n";

       my $table  = SAD->new($db->{$dbname});
       $table->delete_record(CGI::param('sad_delkey'));
       $table->commit;
    }
  }


  #
  # Add new entries
  #
  if ($action eq 'new') {
    my $table = SAD->new($db->{$dbname}, 'ro');
    $table->SAD::CGI::print_new_entry_form();
    return
  }

  if ($action eq 'add') {
    my $table = SAD->new($db->{$dbname}, 'silent');

    my $keyfield = $table->{'fieldlist'}[1];
    my $key = CGI::param($keyfield);

    my %new_record = ();
    foreach my $field ($table->listfields()) {
      $new_record{$field} = CGI::param($field);
      print "<li>Adding field \'$field\' for key \'$key\' as ", CGI::param($field), "\n";
    }
    my $res = $table->add_record($key, %new_record);
      if (!$res) {
        print "<h1>ERROR Adding Record:</h1>\n";
        print "<pre>$SAD::error_messages</pre>\n";
        $table->abort();
      } else {
        $table->commit();
      }  
    CGI::delete_all();
    $table->SAD::CGI::print_html_table($mode);
    exit;
  }


  #
  # Edit records
  #
  if ($action eq 'edit') {
    if (!CGI::param('sad_editkey')) {
      print "<h2>No Edit Key given</h2>\n";
      return;
    } else {
       my $table = SAD->new($db->{$dbname}, "ro");
       $table->SAD::CGI::print_edit_form(CGI::param('sad_editkey'));
       return
    }
  }

  if ($action eq 'submit') {
    print "<h1 align=\"center\">Submitting</h1>\n";
    my $table = SAD->new($db->{$dbname}, 'ro');
    print "DBNAME: $db, $dbname, ", $db->{$dbname}, "-\n";
    print "FIELDS: ", $table->{'nrofields'}, "-\n";
    my $key = $table->{'fieldlist'}[1];
      $key = $table->$key(CGI::param($key));
    $table->SAD::CGI::print_submit_confirm($key);
    exit;
  }

  if ($action eq 'really submit') {
    print "<h1 align=\"center\">Submitting</h1>\n";
    my $table = SAD->new($db->{$dbname}, 'silent');
    my $keyfield = $table->{'fieldlist'}[1];
    my $key = $table->$keyfield(CGI::param($keyfield));

    foreach my $field ($table->listfields()) {
      my $res = $table->set($key, $field, CGI::param($field));
      print "<li>Setting field \'$field\' for key \'$key\' to ", CGI::param($field), "\n";
      if (!$res) {
        print "<h1>ERROR in SUBMIT</h1>\n";
        print "<pre>$SAD::error_messages</pre>\n";
      }
    }
    $table->commit();
    CGI::delete_all();
    # $table->abort();
    $table->SAD::CGI::print_html_table($mode);
    exit;
  }

  # Show the database
  if ($dbname) {
    my $table  = SAD->new($db->{$dbname}, 'ro');
    $table->SAD::CGI::print_html_table($mode);
  } else {
   print "Content-Type: text/html\n\n";
   print CGI::start_form({-method => 'get'});
   my @values = keys %$db;
   print CGI::popup_menu(-name   => 'db',
                           -values => \@values);
   print CGI::submit();
   print CGI::end_form();
  }
}


1;
