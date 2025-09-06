/**
 * CORRECTED SCHEDULING ENGINE - No Conflicts
 * Implements proper resource management and operation dependencies
 */

// Configuration
const CONFIG = {
  MAX_CONCURRENT_SETUPS: 1, // Only ONE operator at a time
  SETUP_WINDOW_START: 6,
  SETUP_WINDOW_END: 22,
  OPERATORS: ['A', 'B', 'C', 'D'],
  MACHINES: ['VMC 1', 'VMC 2', 'VMC 3', 'VMC 4', 'VMC 5', 'VMC 6', 'VMC 7'],
  // Smart Batch Splitting Configuration
  SMART_SPLITTING: {
    MAX_BATCH_SIZE: 300, // Maximum pieces per batch for optimal performance
    PRIORITY_MULTIPLIERS: {
      'Urgent': 3.0,    // Urgent = 3x more batches
      'High': 2.0,      // High = 2x more batches  
      'Normal': 1.0,    // Normal = standard splitting
      'Low': 0.7        // Low = fewer batches (larger batches)
    },
    DEADLINE_URGENCY_THRESHOLD: 2, // Days - if deadline is within 2 days, increase splitting
    DEADLINE_MULTIPLIER: 1.5       // 1.5x more batches when deadline is near
  }
};

// Logger
const Logger = {
  log: function(message) {
      console.log(`[SCHEDULER] ${message}`);
  }
};

// Corrected scheduling engine with proper conflict prevention
class CorrectedSchedulingEngine {
  constructor() {
    this.machineSchedule = {}; // Track all machine bookings
    this.operatorSchedule = {}; // Track all operator bookings
    this.batchSequences = {}; // Track operation sequences per batch
    this.resetSchedules();
  }

  resetSchedules() {
    CONFIG.MACHINES.forEach(machine => {
      this.machineSchedule[machine] = [];
    });
    
    CONFIG.OPERATORS.forEach(operator => {
      this.operatorSchedule[operator] = [];
    });
  }

  // Smart Batch Splitting Algorithm
  calculateSmartBatchSplitting(totalQty, priority, dueDate, startDate) {
    const config = CONFIG.SMART_SPLITTING;
    
    // Calculate days until deadline
    const start = new Date(startDate);
    const due = new Date(dueDate);
    const daysUntilDeadline = Math.ceil((due - start) / (1000 * 60 * 60 * 24));
    
    // Get priority multiplier
    const priorityMultiplier = config.PRIORITY_MULTIPLIERS[priority] || 1.0;
    
    // Check if deadline is urgent
    const isDeadlineUrgent = daysUntilDeadline <= config.DEADLINE_URGENCY_THRESHOLD;
    const deadlineMultiplier = isDeadlineUrgent ? config.DEADLINE_MULTIPLIER : 1.0;
    
    // Calculate optimal batch size (use 300 as base, adjust based on priority)
    let optimalBatchSize = config.MAX_BATCH_SIZE; // 300 pieces
    
    // Adjust batch size based on priority and deadline
    if (priority === 'Urgent' || isDeadlineUrgent) {
      optimalBatchSize = Math.floor(config.MAX_BATCH_SIZE / 2); // 150 pieces
    } else if (priority === 'High') {
      optimalBatchSize = Math.floor(config.MAX_BATCH_SIZE * 0.7); // 210 pieces
    } else if (priority === 'Low') {
      optimalBatchSize = Math.min(config.MAX_BATCH_SIZE * 1.5, totalQty); // Up to 450 pieces
    }
    
    // Apply multipliers for more aggressive splitting
    const finalMultiplier = priorityMultiplier * deadlineMultiplier;
    if (finalMultiplier > 1.0) {
      optimalBatchSize = Math.floor(optimalBatchSize / finalMultiplier);
    }
    
    // Calculate number of batches needed
    const numBatches = Math.ceil(totalQty / optimalBatchSize);
    
    // Create batches
    const batches = [];
    let remainingQty = totalQty;
    
    for (let i = 0; i < numBatches; i++) {
      const batchQty = Math.min(optimalBatchSize, remainingQty);
      const batchId = `B${String(i + 1).padStart(2, '0')}`;
      
      batches.push({
        qty: batchQty,
        batchId: batchId
      });
      
      remainingQty -= batchQty;
      if (remainingQty <= 0) break;
    }
    
    // Log the smart splitting decision
    Logger.log(`[SMART-SPLITTING] ${totalQty} pieces → ${batches.length} batches (Priority: ${priority}, Days until deadline: ${daysUntilDeadline}, Urgent: ${isDeadlineUrgent})`);
    batches.forEach(batch => {
      Logger.log(`[SMART-SPLITTING] ${batch.batchId}: ${batch.qty} pieces`);
    });
    
    return batches;
  }

  // Get operator for operation based on batch and operation sequence
  getSequentialOperator(operationSeq, batchId) {
    // Handle specific patterns for B01 and B02 (maintain compatibility)
    if (batchId === 'B01') {
      const pattern = ['A', 'B', 'A', 'B', 'A']; // B01 pattern: A, B, A, B, A
      return pattern[(operationSeq - 1) % 5];
    } else if (batchId === 'B02') {
      const pattern = ['B', 'A', 'C', 'D', 'B']; // B02 pattern: B, A, C, D, B
      return pattern[(operationSeq - 1) % 5];
    }
    
    // Smart operator assignment for additional batches
    const batchNumber = parseInt(batchId.replace('B', ''));
    const operatorPatterns = [
      ['A', 'B', 'A', 'B', 'A'], // B01, B05, B09...
      ['B', 'A', 'C', 'D', 'B'], // B02, B06, B10...
      ['C', 'D', 'C', 'D', 'C'], // B03, B07, B11...
      ['D', 'C', 'A', 'B', 'D']  // B04, B08, B12...
    ];
    
    const patternIndex = (batchNumber - 1) % 4;
    const pattern = operatorPatterns[patternIndex];
    return pattern[(operationSeq - 1) % 5];
  }

  // Check if machine is available for the given time range
  // STRICT MACHINE LOCKING: Machine locks from Setup Start → Run End
  isMachineAvailable(machine, startTime, endTime) {
    const bookings = this.machineSchedule[machine] || [];
    
    for (const booking of bookings) {
      const bookingStart = new Date(booking.start);
      const bookingEnd = new Date(booking.end);
      const requestedStart = new Date(startTime);
      const requestedEnd = new Date(endTime);
      
      // Check for ANY overlap - machine is locked from setup start to run end
      if (requestedStart < bookingEnd && requestedEnd > bookingStart) {
        Logger.log(`[MACHINE-CONFLICT] ${machine} unavailable: ${booking.batchId} Op${booking.opSeq} (${bookingStart.toISOString()} - ${bookingEnd.toISOString()}) conflicts with requested (${requestedStart.toISOString()} - ${requestedEnd.toISOString()})`);
        return false;
      }
    }
    return true; // Machine is available
  }

  // Check if operator is available for the given time range
  isOperatorAvailable(operator, startTime, endTime) {
    const bookings = this.operatorSchedule[operator] || [];
    
    for (const booking of bookings) {
      const bookingStart = new Date(booking.start);
      const bookingEnd = new Date(booking.end);
      const requestedStart = new Date(startTime);
      const requestedEnd = new Date(endTime);
      
      // Check for overlap
      if (requestedStart < bookingEnd && requestedEnd > bookingStart) {
        return false; // Operator is busy
      }
    }
    return true; // Operator is available
  }

  // Find available machine from eligible machines
  findAvailableMachine(eligibleMachines, startTime, endTime) {
    for (const machine of eligibleMachines) {
      if (this.isMachineAvailable(machine, startTime, endTime)) {
        return machine;
      }
    }
    return null; // No machine available
  }

  // Find available operator for the time range
  findAvailableOperator(operator, startTime, endTime) {
    if (this.isOperatorAvailable(operator, startTime, endTime)) {
      return operator;
    }
    
    // Try other operators in the same shift
    const shiftOperators = this.getShiftOperators(startTime);
    for (const altOperator of shiftOperators) {
      if (altOperator !== operator && this.isOperatorAvailable(altOperator, startTime, endTime)) {
        return altOperator;
      }
    }
    
    return null; // No operator available
  }

  // Get operators for the shift based on time
  getShiftOperators(time) {
    const hour = new Date(time).getHours();
    if (hour >= 6 && hour < 14) {
      return ['A', 'B']; // Morning shift
    } else if (hour >= 14 && hour < 22) {
      return ['C', 'D']; // Afternoon shift
    } else {
      return []; // No operators available outside setup window
    }
  }

  // Priority-based machine queueing system
  // Implements: Priority > Due Date > FIFO ordering
  getMachineQueueOrder(batches, machine) {
    return batches.sort((a, b) => {
      // 1. Priority first (Urgent > High > Normal > Low)
      const priorityOrder = { 'Urgent': 4, 'High': 3, 'Normal': 2, 'Low': 1 };
      const priorityA = priorityOrder[a.priority] || 0;
      const priorityB = priorityOrder[b.priority] || 0;
      
      if (priorityA !== priorityB) {
        return priorityB - priorityA; // Higher priority first
      }
      
      // 2. If same priority, sort by earliest due date
      const dueDateA = new Date(a.dueDate);
      const dueDateB = new Date(b.dueDate);
      if (dueDateA.getTime() !== dueDateB.getTime()) {
        return dueDateA - dueDateB; // Earlier due date first
      }
      
      // 3. If still tied, use FIFO (batch creation order)
      return a.batchId.localeCompare(b.batchId);
    });
  }

  // Find next available time for a machine considering all queued batches
  findNextAvailableMachineTime(machine, requestedStart, requestedEnd) {
    const bookings = this.machineSchedule[machine] || [];
    
    // Sort bookings by start time
    const sortedBookings = bookings.sort((a, b) => new Date(a.start) - new Date(b.start));
    
    let candidateStart = new Date(requestedStart);
    const requestedDuration = requestedEnd - requestedStart;
    
    for (const booking of sortedBookings) {
      const bookingStart = new Date(booking.start);
      const bookingEnd = new Date(booking.end);
      
      // Check if we can fit before this booking
      if (candidateStart.getTime() + requestedDuration <= bookingStart.getTime()) {
        return candidateStart; // We can fit before this booking
      }
      
      // Otherwise, try to start after this booking ends
      candidateStart = new Date(bookingEnd);
    }
    
    return candidateStart; // Start after all existing bookings
  }

  // Calculate when resources will be available
  calculateResourceAvailability(machine, operator, earliestStart) {
    let machineAvailableFrom = new Date(earliestStart);
    let operatorAvailableFrom = new Date(earliestStart);
    
    // Find when machine becomes available
    const machineBookings = this.machineSchedule[machine] || [];
    for (const booking of machineBookings) {
      const bookingEnd = new Date(booking.end);
      if (bookingEnd > machineAvailableFrom) {
        machineAvailableFrom = new Date(bookingEnd);
      }
    }
    
    // Find when operator becomes available
    const operatorBookings = this.operatorSchedule[operator] || [];
    for (const booking of operatorBookings) {
      const bookingEnd = new Date(booking.end);
      if (bookingEnd > operatorAvailableFrom) {
        operatorAvailableFrom = new Date(bookingEnd);
      }
    }
    
    // Return the later of the two
    return machineAvailableFrom > operatorAvailableFrom ? machineAvailableFrom : operatorAvailableFrom;
  }

  // Get available resources with conflict checking
  getAvailableResources(operationSeq, batchId, earliestStart, setupTimeMin, cycleTimeMin, batchQty, eligibleMachines, machineSchedule, operatorSchedule) {
    const conflicts = [];
    
    // Get preferred operator
    let operator = this.getSequentialOperator(operationSeq, batchId);
    
    // Calculate setup and run times
    const setupDuration = setupTimeMin * 60 * 1000; // Convert to milliseconds
    const runDuration = cycleTimeMin * batchQty * 60 * 1000; // Convert to milliseconds
    
    // Start with earliest possible time
    let setupStart = new Date(earliestStart);
    
    // Ensure setup is within setup window
    const setupStartHour = setupStart.getHours();
    if (setupStartHour < CONFIG.SETUP_WINDOW_START) {
      setupStart.setHours(CONFIG.SETUP_WINDOW_START, 0, 0, 0);
    } else if (setupStartHour >= CONFIG.SETUP_WINDOW_END) {
      // Move to next day
      setupStart.setDate(setupStart.getDate() + 1);
      setupStart.setHours(CONFIG.SETUP_WINDOW_START, 0, 0, 0);
    }
    
    // Find available machine
    let selectedMachine = null;
    let attempts = 0;
    const maxAttempts = 10;
    
    while (!selectedMachine && attempts < maxAttempts) {
      const setupEnd = new Date(setupStart.getTime() + setupDuration);
      const runEnd = new Date(setupEnd.getTime() + runDuration);
      
      // Try to find available machine
      selectedMachine = this.findAvailableMachine(eligibleMachines, setupStart, runEnd);
      
      if (!selectedMachine) {
        // Machine conflict - delay setup
        setupStart.setMinutes(setupStart.getMinutes() + 30); // Delay by 30 minutes
        conflicts.push('machine_conflict');
        attempts++;
      }
    }
    
    if (!selectedMachine) {
      selectedMachine = eligibleMachines[0]; // Fallback
      conflicts.push('machine_unavailable');
    }
    
    // Find available operator
    const setupEnd = new Date(setupStart.getTime() + setupDuration);
    let availableOperator = this.findAvailableOperator(operator, setupStart, setupEnd);
    
    if (!availableOperator) {
      // Operator conflict - try alternative operators
      const shiftOperators = this.getShiftOperators(setupStart);
      for (const altOperator of shiftOperators) {
        if (this.isOperatorAvailable(altOperator, setupStart, setupEnd)) {
          availableOperator = altOperator;
          conflicts.push('operator_conflict');
          break;
        }
      }
      
      if (!availableOperator) {
        // Delay setup until operator is available
        const operatorAvailability = this.calculateResourceAvailability(selectedMachine, operator, setupStart);
        setupStart = new Date(operatorAvailability);
        availableOperator = operator;
        conflicts.push('operator_delay');
      }
    }
    
    // Calculate final times
    const finalSetupEnd = new Date(setupStart.getTime() + setupDuration);
    const runStart = new Date(finalSetupEnd);
    const runEnd = new Date(runStart.getTime() + runDuration);
    
    // Format timing
    const totalMinutes = Math.round((runEnd - setupStart) / (60 * 1000));
    const hours = Math.floor(totalMinutes / 60);
    const minutes = totalMinutes % 60;
    const timing = `${hours}H ${minutes}M`;
    
    return {
      selectedMachine,
      operator: availableOperator,
      setupStart: setupStart.toISOString().replace('T', ' ').substring(0, 16),
      setupEnd: finalSetupEnd.toISOString().replace('T', ' ').substring(0, 16),
      runStart: runStart.toISOString().replace('T', ' ').substring(0, 16),
      runEnd: runEnd.toISOString().replace('T', ' ').substring(0, 16),
      timing,
      conflicts
    };
  }

  // CORRECTED: Enhanced resource assignment with proper machine locking, person serialization, and sequence enforcement
  getAvailableResourcesWithQueueing(operationSeq, batchId, earliestStart, setupTimeMin, cycleTimeMin, batchQty, eligibleMachines, machineSchedule, operatorSchedule, priority, dueDate) {
    const conflicts = [];
    
    Logger.log(`[CORRECTED-RESOURCE] ${batchId} Op${operationSeq} (${priority} priority, due ${dueDate}) - Enforcing strict rules`);
    
    // STEP 1: Find available machine with proper locking
    let selectedMachine = null;
    let machineAvailableFrom = new Date(earliestStart);
    
    for (const machine of eligibleMachines) {
      const machineAvailability = this.findMachineAvailability(machine, earliestStart, setupTimeMin, cycleTimeMin, batchQty);
      
      if (machineAvailability.available) {
        selectedMachine = machine;
        machineAvailableFrom = machineAvailability.availableFrom;
        Logger.log(`[MACHINE-LOCK] ${machine} available from ${machineAvailableFrom.toISOString()}`);
        break;
      } else {
        conflicts.push(`Machine ${machine} locked until ${machineAvailability.nextAvailable.toISOString()}`);
      }
    }
    
    if (!selectedMachine) {
      // All machines busy - use first one with earliest availability
      selectedMachine = eligibleMachines[0];
      const fallbackAvailability = this.findMachineAvailability(selectedMachine, earliestStart, setupTimeMin, cycleTimeMin, batchQty);
      machineAvailableFrom = fallbackAvailability.nextAvailable;
      conflicts.push(`FALLBACK: All machines busy, using ${selectedMachine} from ${machineAvailableFrom.toISOString()}`);
    }
    
    // STEP 2: Find available operator with proper serialization
    let selectedOperator = null;
    let operatorAvailableFrom = new Date(machineAvailableFrom);
    
    // Try preferred operator first
    const preferredOperator = this.getSequentialOperator(operationSeq, batchId);
    const operatorAvailability = this.findOperatorAvailability(preferredOperator, machineAvailableFrom, setupTimeMin);
    
    if (operatorAvailability.available) {
      selectedOperator = preferredOperator;
      operatorAvailableFrom = operatorAvailability.availableFrom;
      Logger.log(`[OPERATOR-SERIAL] ${preferredOperator} available from ${operatorAvailableFrom.toISOString()}`);
    } else {
      // Try alternative operators in same shift
      const shiftOperators = this.getShiftOperators(machineAvailableFrom);
      for (const operator of shiftOperators) {
        const altAvailability = this.findOperatorAvailability(operator, machineAvailableFrom, setupTimeMin);
        if (altAvailability.available) {
          selectedOperator = operator;
          operatorAvailableFrom = altAvailability.availableFrom;
          conflicts.push(`Operator ${preferredOperator} busy, using ${operator} from ${operatorAvailableFrom.toISOString()}`);
          break;
        }
      }
      
      if (!selectedOperator) {
        // All operators busy - delay until next available
        const earliestOperatorTime = this.findEarliestOperatorAvailability(machineAvailableFrom, setupTimeMin);
        selectedOperator = earliestOperatorTime.operator;
        operatorAvailableFrom = earliestOperatorTime.availableFrom;
        conflicts.push(`All operators busy, delayed to ${operatorAvailableFrom.toISOString()}`);
      }
    }
    
    // STEP 3: Calculate final timing with proper sequencing
    const finalStartTime = new Date(Math.max(machineAvailableFrom.getTime(), operatorAvailableFrom.getTime()));
    const setupStart = finalStartTime;
    const setupEnd = new Date(setupStart.getTime() + setupTimeMin * 60000);
    const runStart = new Date(setupEnd);
    const runEnd = new Date(runStart.getTime() + cycleTimeMin * batchQty * 60000);
    
    // STEP 4: Validate sequence integrity
    const sequenceValidation = this.validateSequenceIntegrity(batchId, operationSeq, setupStart, runEnd);
    if (!sequenceValidation.valid) {
      conflicts.push(`SEQUENCE-VIOLATION: ${sequenceValidation.message}`);
      // Adjust timing to maintain sequence
      const adjustedStart = sequenceValidation.correctedStart;
      const adjustedSetupEnd = new Date(adjustedStart.getTime() + setupTimeMin * 60000);
      const adjustedRunStart = new Date(adjustedSetupEnd);
      const adjustedRunEnd = new Date(adjustedRunStart.getTime() + cycleTimeMin * batchQty * 60000);
      
      Logger.log(`[SEQUENCE-FIX] ${batchId} Op${operationSeq} adjusted from ${setupStart.toISOString()} to ${adjustedStart.toISOString()}`);
      
      return {
        selectedMachine: selectedMachine,
        operator: selectedOperator,
        setupStart: adjustedStart.toISOString().replace('T', ' ').substring(0, 16),
        setupEnd: adjustedSetupEnd.toISOString().replace('T', ' ').substring(0, 16),
        runStart: adjustedRunStart.toISOString().replace('T', ' ').substring(0, 16),
        runEnd: adjustedRunEnd.toISOString().replace('T', ' ').substring(0, 16),
        timing: this.calculateTiming(adjustedStart, adjustedRunEnd),
        conflicts: conflicts
      };
    }
    
    Logger.log(`[CORRECTED-RESOURCE] ${batchId} Op${operationSeq} assigned to ${selectedMachine} with operator ${selectedOperator}`);
    
    return {
      selectedMachine: selectedMachine,
      operator: selectedOperator,
      setupStart: setupStart.toISOString().replace('T', ' ').substring(0, 16),
      setupEnd: setupEnd.toISOString().replace('T', ' ').substring(0, 16),
      runStart: runStart.toISOString().replace('T', ' ').substring(0, 16),
      runEnd: runEnd.toISOString().replace('T', ' ').substring(0, 16),
      timing: this.calculateTiming(setupStart, runEnd),
      conflicts: conflicts
    };
  }

  // Helper function to find machine availability with proper locking
  findMachineAvailability(machine, earliestStart, setupTimeMin, cycleTimeMin, batchQty) {
    const bookings = this.machineSchedule[machine] || [];
    const setupDuration = setupTimeMin * 60000;
    const runDuration = cycleTimeMin * batchQty * 60000;
    const totalDuration = setupDuration + runDuration;
    
    // Sort bookings by start time
    const sortedBookings = bookings.sort((a, b) => new Date(a.start) - new Date(b.start));
    
    let candidateStart = new Date(earliestStart);
    
    for (const booking of sortedBookings) {
      const bookingStart = new Date(booking.start);
      const bookingEnd = new Date(booking.end);
      
      // Check if we can fit before this booking
      if (candidateStart.getTime() + totalDuration <= bookingStart.getTime()) {
        return {
          available: true,
          availableFrom: candidateStart,
          nextAvailable: new Date(bookingEnd)
        };
      }
      
      // Otherwise, try to start after this booking ends
      candidateStart = new Date(bookingEnd);
    }
    
    // Check if we can start now
    if (candidateStart.getTime() <= earliestStart.getTime()) {
      return {
        available: true,
        availableFrom: earliestStart,
        nextAvailable: new Date(earliestStart.getTime() + totalDuration)
      };
    }
    
    return {
      available: false,
      availableFrom: null,
      nextAvailable: candidateStart
    };
  }

  // Helper function to find operator availability with proper serialization
  findOperatorAvailability(operator, earliestStart, setupTimeMin) {
    const bookings = this.operatorSchedule[operator] || [];
    const setupDuration = setupTimeMin * 60000;
    
    // Sort bookings by start time
    const sortedBookings = bookings.sort((a, b) => new Date(a.start) - new Date(b.start));
    
    let candidateStart = new Date(earliestStart);
    
    for (const booking of sortedBookings) {
      const bookingStart = new Date(booking.start);
      const bookingEnd = new Date(booking.end);
      
      // Check if we can fit before this booking
      if (candidateStart.getTime() + setupDuration <= bookingStart.getTime()) {
        return {
          available: true,
          availableFrom: candidateStart,
          nextAvailable: new Date(bookingEnd)
        };
      }
      
      // Otherwise, try to start after this booking ends
      candidateStart = new Date(bookingEnd);
    }
    
    // Check if we can start now
    if (candidateStart.getTime() <= earliestStart.getTime()) {
      return {
        available: true,
        availableFrom: earliestStart,
        nextAvailable: new Date(earliestStart.getTime() + setupDuration)
      };
    }
    
    return {
      available: false,
      availableFrom: null,
      nextAvailable: candidateStart
    };
  }

  // Helper function to find earliest operator availability across all operators
  findEarliestOperatorAvailability(earliestStart, setupTimeMin) {
    let earliestTime = null;
    let earliestOperator = null;
    
    CONFIG.OPERATORS.forEach(operator => {
      const availability = this.findOperatorAvailability(operator, earliestStart, setupTimeMin);
      if (availability.available) {
        if (!earliestTime || availability.availableFrom < earliestTime) {
          earliestTime = availability.availableFrom;
          earliestOperator = operator;
        }
      }
    });
    
    return {
      operator: earliestOperator || 'A',
      availableFrom: earliestTime || new Date(earliestStart.getTime() + 24 * 60 * 60 * 1000) // Next day fallback
    };
  }

  // Helper function to validate sequence integrity
  validateSequenceIntegrity(batchId, operationSeq, setupStart, runEnd) {
    // Check if this is the first operation
    if (operationSeq === 1) {
      return { valid: true, message: 'First operation', correctedStart: setupStart };
    }
    
    // Find previous operation for this batch
    const previousOpSeq = operationSeq - 1;
    const previousOperation = this.findPreviousOperation(batchId, previousOpSeq);
    
    if (!previousOperation) {
      return { valid: true, message: 'No previous operation found', correctedStart: setupStart };
    }
    
    const previousRunEnd = new Date(previousOperation.runEnd);
    
    // Check if current operation starts before previous operation ends
    if (setupStart < previousRunEnd) {
      const correctedStart = new Date(previousRunEnd);
      return {
        valid: false,
        message: `Op${operationSeq} starts at ${setupStart.toISOString()} before Op${previousOpSeq} ends at ${previousRunEnd.toISOString()}`,
        correctedStart: correctedStart
      };
    }
    
    return { valid: true, message: 'Sequence integrity maintained', correctedStart: setupStart };
  }

  // Helper function to find previous operation for sequence validation
  findPreviousOperation(batchId, operationSeq) {
    // Look in the batchSequences tracking
    if (this.batchSequences && this.batchSequences[batchId]) {
      const batchOps = this.batchSequences[batchId].operations || [];
      const previousOp = batchOps.find(op => op.operationSeq === operationSeq);
      return previousOp ? {
        runEnd: previousOp.runEnd,
        operationSeq: previousOp.operationSeq,
        machine: previousOp.machine
      } : null;
    }
    return null;
  }

  // Book machine resource
  bookMachine(machineSchedule, machine, startTime, endTime) {
    if (!machineSchedule[machine]) {
      machineSchedule[machine] = [];
    }
    
    machineSchedule[machine].push({
      start: startTime,
      end: endTime,
      type: 'booking'
    });
  }

  // Book operator resource
  bookOperator(operatorSchedule, operator, startTime, endTime) {
    if (!operatorSchedule[operator]) {
      operatorSchedule[operator] = [];
    }
    
    operatorSchedule[operator].push({
      start: startTime,
      end: endTime,
      type: 'setup'
    });
  }

  // Comprehensive conflict prevention validation
  // Checks: Machine conflicts, Operator conflicts, Sequence violations, Timeline overlaps
  validateSchedule(results, machineSchedule, operatorSchedule) {
    const conflicts = [];
    const warnings = [];
    
    Logger.log(`[VALIDATION] Starting comprehensive conflict check...`);
    
    // 1. MACHINE CONFLICT CHECK - STRICT LOCKING
    CONFIG.MACHINES.forEach(machine => {
      const bookings = machineSchedule[machine] || [];
      Logger.log(`[VALIDATION] Checking ${machine}: ${bookings.length} bookings`);
      
      for (let i = 0; i < bookings.length; i++) {
        for (let j = i + 1; j < bookings.length; j++) {
          const booking1 = bookings[i];
          const booking2 = bookings[j];
          
          const start1 = new Date(booking1.start);
          const end1 = new Date(booking1.end);
          const start2 = new Date(booking2.start);
          const end2 = new Date(booking2.end);
          
          // Check for ANY overlap (machine locks from setup start to run end)
          if (start1 < end2 && start2 < end1) {
            const overlapStart = new Date(Math.max(start1.getTime(), start2.getTime()));
            const overlapEnd = new Date(Math.min(end1.getTime(), end2.getTime()));
            const overlapDuration = Math.round((overlapEnd - overlapStart) / (1000 * 60)); // minutes
            
            conflicts.push({
              type: 'MACHINE_CONFLICT',
              severity: 'CRITICAL',
              machine: machine,
              message: `Machine ${machine} double-booked: ${booking1.batchId} Op${booking1.opSeq} (${start1.toISOString()}→${end1.toISOString()}) overlaps with ${booking2.batchId} Op${booking2.opSeq} (${start2.toISOString()}→${end2.toISOString()}) - Overlap: ${overlapDuration} minutes`,
              overlap: { start: overlapStart, end: overlapEnd, duration: overlapDuration }
            });
          }
        }
      }
    });
    
    // 2. OPERATOR CONFLICT CHECK
    CONFIG.OPERATORS.forEach(operator => {
      const bookings = operatorSchedule[operator] || [];
      Logger.log(`[VALIDATION] Checking Operator ${operator}: ${bookings.length} bookings`);
      
      for (let i = 0; i < bookings.length; i++) {
        for (let j = i + 1; j < bookings.length; j++) {
          const booking1 = bookings[i];
          const booking2 = bookings[j];
          
          const start1 = new Date(booking1.start);
          const end1 = new Date(booking1.end);
          const start2 = new Date(booking2.start);
          const end2 = new Date(booking2.end);
          
          if (start1 < end2 && start2 < end1) {
            conflicts.push({
              type: 'OPERATOR_CONFLICT',
              severity: 'CRITICAL',
              operator: operator,
              message: `Operator ${operator} double-booked: ${booking1.batchId} Op${booking1.opSeq} (${start1.toISOString()}→${end1.toISOString()}) overlaps with ${booking2.batchId} Op${booking2.opSeq} (${start2.toISOString()}→${end2.toISOString()})`
            });
          }
        }
      }
    });
    
    // 3. SEQUENCE VIOLATION CHECK
    const batchSequences = {};
    results.forEach(row => {
      const batchId = row[3]; // Batch ID
      const opSeq = row[5]; // Operation Sequence
      const runEnd = new Date(row[12]); // Run End
      
      if (!batchSequences[batchId]) {
        batchSequences[batchId] = [];
      }
      batchSequences[batchId].push({ opSeq, runEnd });
    });
    
    Object.keys(batchSequences).forEach(batchId => {
      const operations = batchSequences[batchId].sort((a, b) => a.opSeq - b.opSeq);
      
      for (let i = 0; i < operations.length - 1; i++) {
        const currentOp = operations[i];
        const nextOp = operations[i + 1];
        
        // Check if next operation starts before current operation ends
        const nextOpStart = new Date(results.find(row => row[3] === batchId && row[5] === nextOp.opSeq)[9]); // Setup Start
        const currentOpEnd = currentOp.runEnd;
        
        if (nextOpStart < currentOpEnd) {
          conflicts.push({
            type: 'SEQUENCE_VIOLATION',
            severity: 'CRITICAL',
            batchId: batchId,
            message: `Sequence violation in ${batchId}: Op${nextOp.opSeq} starts at ${nextOpStart.toISOString()} before Op${currentOp.opSeq} ends at ${currentOpEnd.toISOString()}`
          });
        }
      }
    });
    
    // 4. TIMELINE CONSISTENCY CHECK
    results.forEach((row, index) => {
      const setupStart = new Date(row[9]);
      const setupEnd = new Date(row[10]);
      const runStart = new Date(row[11]);
      const runEnd = new Date(row[12]);
      
      // Check setup timing consistency
      if (setupEnd <= setupStart) {
        warnings.push({
          type: 'TIMELINE_WARNING',
          severity: 'WARNING',
          message: `Invalid setup timing in row ${index}: Setup ends before it starts (${setupStart.toISOString()} → ${setupEnd.toISOString()})`
        });
      }
      
      // Check run timing consistency
      if (runEnd <= runStart) {
        warnings.push({
          type: 'TIMELINE_WARNING',
          severity: 'WARNING',
          message: `Invalid run timing in row ${index}: Run ends before it starts (${runStart.toISOString()} → ${runEnd.toISOString()})`
        });
      }
      
      // Check setup-run continuity
      if (runStart < setupEnd) {
        warnings.push({
          type: 'TIMELINE_WARNING',
          severity: 'WARNING',
          message: `Setup-run gap in row ${index}: Run starts before setup ends (Setup: ${setupStart.toISOString()}→${setupEnd.toISOString()}, Run: ${runStart.toISOString()}→${runEnd.toISOString()})`
        });
      }
    });
    
    const validationResult = {
      valid: conflicts.length === 0,
      conflicts: conflicts,
      warnings: warnings,
      summary: {
        totalConflicts: conflicts.length,
        criticalConflicts: conflicts.filter(c => c.severity === 'CRITICAL').length,
        warnings: warnings.length,
        machinesChecked: CONFIG.MACHINES.length,
        operatorsChecked: CONFIG.OPERATORS.length,
        batchesChecked: Object.keys(batchSequences).length
      }
    };
    
    Logger.log(`[VALIDATION] Complete: ${validationResult.summary.totalConflicts} conflicts, ${validationResult.summary.warnings} warnings`);
    
    return validationResult;
  }

  // Main scheduling function
  scheduleOperations(inputData, operationMaster) {
    Logger.log('=== CORRECTED SCHEDULING ENGINE STARTED ===');
    
    const results = [];
    const globalStart = new Date('2025-09-01 06:00:00');
    
    // Enhanced resource tracking with proper locking
    const machineSchedule = {}; // Track all machine bookings
    const operatorSchedule = {}; // Track all operator bookings
    const batchSequences = {}; // Track operation sequences per batch
    
    // Store references for helper functions
    this.machineSchedule = machineSchedule;
    this.operatorSchedule = operatorSchedule;
    this.batchSequences = batchSequences;
    
    // Initialize resource schedules
    CONFIG.MACHINES.forEach(machine => {
      machineSchedule[machine] = [];
    });
    
    CONFIG.OPERATORS.forEach(operator => {
      operatorSchedule[operator] = [];
    });
    
    // Collect all batches first for priority-based scheduling
    const allBatches = [];
    inputData.forEach(order => {
      const operations = operationMaster.filter(op => op.PartNumber === order.partNumber);
      operations.sort((a, b) => a.OperationSeq - b.OperationSeq);
      
      Logger.log(`Processing ${order.partNumber} with ${operations.length} operations`);
      
      // Smart Batch Splitting
      const totalQty = order.orderQty || order.quantity || 1;
      const priority = order.priority || 'Normal';
      const dueDate = order.dueDate || '2025-09-06';
      const startDate = order.startDateTime || '2025-09-01 06:00';
      
      const batches = this.calculateSmartBatchSplitting(totalQty, priority, dueDate, startDate);
      
      // Add order info to each batch
      batches.forEach(batch => {
        batch.partNumber = order.partNumber;
        batch.priority = priority;
        batch.dueDate = dueDate;
        batch.startDate = startDate;
        batch.operations = operations;
      });
      
      allBatches.push(...batches);
    });
    
    // Sort all batches by priority queueing policy: Priority > Due Date > FIFO
    const sortedBatches = this.getMachineQueueOrder(allBatches);
    Logger.log(`[QUEUEING] Processing ${sortedBatches.length} batches in priority order`);
    
    // Process each batch with strict sequence enforcement and machine locking
    sortedBatches.forEach((batch, batchIndex) => {
      Logger.log(`[QUEUEING] Processing batch ${batch.batchId} (${batch.priority} priority, due ${batch.dueDate})`);
      
      // Initialize batch sequence tracking
      batchSequences[batch.batchId] = {
        lastOperationEnd: globalStart,
        operations: []
      };
      
      // Process operations in strict sequence order
      batch.operations.forEach((op, index) => {
        const operationSeq = op.OperationSeq;
        const operationName = op.OperationName;
        const setupTimeMin = op.SetupTime_Min;
        const cycleTimeMin = op.CycleTime_Min;
        const eligibleMachines = op.EligibleMachines || ['VMC 1'];
        
        // Get available resources with STRICT machine locking and priority queueing
        const resourceAssignment = this.getAvailableResourcesWithQueueing(
          operationSeq, 
          batch.batchId, 
          batchSequences[batch.batchId].lastOperationEnd,
          setupTimeMin,
          cycleTimeMin,
          batch.qty,
          eligibleMachines,
          machineSchedule,
          operatorSchedule,
          batch.priority,
          batch.dueDate
        );
          
          const { selectedMachine, operator, setupStart, setupEnd, runStart, runEnd, timing, conflicts } = resourceAssignment;
          
          // Log conflicts if any
          if (conflicts.length > 0) {
            Logger.log(`[CONFLICT-RESOLVED] ${batch.batchId} Op${operationSeq}: ${conflicts.join(', ')}`);
          }
          
          // Book resources
          this.bookMachine(machineSchedule, selectedMachine, setupStart, runEnd);
          this.bookOperator(operatorSchedule, operator, setupStart, setupEnd);
          
          // Update batch sequence tracking
          batchSequences[batch.batchId].lastOperationEnd = new Date(runEnd);
          batchSequences[batch.batchId].operations.push({
            operationSeq,
            machine: selectedMachine,
            operator,
            setupStart,
            setupEnd,
            runStart,
            runEnd
          });
          
          Logger.log(`[SCHEDULED] ${batch.batchId} Op${operationSeq}: ${selectedMachine} ${operator} Setup ${setupStart}→${setupEnd} | Run ${runStart}→${runEnd} | ${timing}`);
          
          // Add to results
          results.push([
            order.partNumber,
            order.orderQty || order.quantity || 1,
            order.priority || 'Normal',
            batch.batchId,
            batch.qty,
            operationSeq,
            operationName,
            selectedMachine,
            operator,
            setupStart,
            setupEnd,
            runStart,
            runEnd,
            timing,
            order.dueDate || '2025-09-06'
          ]);
        });
      });
    });
    
    // Validate final schedule for conflicts
    const validationResult = this.validateSchedule(results, machineSchedule, operatorSchedule);
    if (!validationResult.valid) {
      Logger.log(`[VALIDATION-FAILED] ${validationResult.conflicts.join(', ')}`);
    }
    
    Logger.log('=== CORRECTED SCHEDULING COMPLETED ===');
    Logger.log(`Total operations scheduled: ${results.length}`);
    
    return {
      success: true,
      outputData: {
        mainOutput: results
      },
      validation: validationResult
    };
  }
}

// Main function for browser use
function runSchedulingInBrowser(inputData, operationMaster, options = {}) {
  const engine = new CorrectedSchedulingEngine();
  return engine.scheduleOperations(inputData, operationMaster);
}
